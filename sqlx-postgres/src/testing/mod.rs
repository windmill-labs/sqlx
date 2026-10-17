use std::fmt::Write;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;

use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use futures_core::future::BoxFuture;
use sha2::{Digest, Sha512};

use once_cell::sync::OnceCell;
use sqlx_core::connection::Connection;
use sqlx_core::query_scalar::query_scalar;

use crate::error::Error;
use crate::executor::Executor;
use crate::pool::{Pool, PoolOptions};
use crate::query::query;
use crate::{PgConnectOptions, PgConnection, Postgres};
use sqlx_core::migrate::Migrator;

pub(crate) use sqlx_core::testing::*;

// Using a blocking `OnceCell` here because the critical sections are short.
static MASTER_POOL: OnceCell<Pool<Postgres>> = OnceCell::new();
// Automatically delete any databases created before the start of the test binary.

impl TestSupport for Postgres {
    fn test_context(args: &TestArgs) -> BoxFuture<'_, Result<TestContext<Self>, Error>> {
        Box::pin(async move { test_context(args).await })
    }

    fn cleanup_test(db_name: &str) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut conn = MASTER_POOL
                .get()
                .expect("cleanup_test() invoked outside `#[sqlx::test]`")
                .acquire()
                .await?;

            do_cleanup(&mut conn, db_name).await
        })
    }

    fn cleanup_test_dbs() -> BoxFuture<'static, Result<Option<usize>, Error>> {
        Box::pin(async move {
            let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

            let mut conn = PgConnection::connect(&url).await?;

            let delete_db_names: Vec<String> =
                query_scalar("select db_name from _sqlx_test.databases")
                    .fetch_all(&mut conn)
                    .await?;

            if delete_db_names.is_empty() {
                return Ok(None);
            }

            let mut deleted_db_names = Vec::with_capacity(delete_db_names.len());

            let mut command = String::new();

            for db_name in &delete_db_names {
                command.clear();
                writeln!(command, "drop database if exists {db_name:?};").ok();
                match conn.execute(&*command).await {
                    Ok(_deleted) => {
                        deleted_db_names.push(db_name);
                    }
                    // Assume a database error just means the DB is still in use.
                    Err(Error::Database(dbe)) => {
                        eprintln!("could not clean test database {db_name:?}: {dbe}")
                    }
                    // Bubble up other errors
                    Err(e) => return Err(e),
                }
            }

            query("delete from _sqlx_test.databases where db_name = any($1::text[])")
                .bind(&deleted_db_names)
                .execute(&mut conn)
                .await?;

            let _ = conn.close().await;
            Ok(Some(delete_db_names.len()))
        })
    }

    fn snapshot(
        _conn: &mut Self::Connection,
    ) -> BoxFuture<'_, Result<FixtureSnapshot<Self>, Error>> {
        // TODO: I want to get the testing feature out the door so this will have to wait,
        // but I'm keeping the code around for now because I plan to come back to it.
        todo!()
    }
}

async fn test_context(args: &TestArgs) -> Result<TestContext<Postgres>, Error> {
    let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let master_opts = PgConnectOptions::from_str(&url).expect("failed to parse DATABASE_URL");

    let pool = PoolOptions::new()
        // Postgres' normal connection limit is 100 plus 3 superuser connections
        // We don't want to use the whole cap and there may be fuzziness here due to
        // concurrently running tests anyway.
        .max_connections(master_max_connections())
        // Immediately close master connections. Tokio's I/O streams don't like hopping runtimes.
        .after_release(|_conn, _| Box::pin(async move { Ok(false) }))
        .connect_lazy_with(master_opts);

    let master_pool = match MASTER_POOL.try_insert(pool) {
        Ok(inserted) => inserted,
        Err((existing, pool)) => {
            // Sanity checks.
            assert_eq!(
                existing.connect_options().host,
                pool.connect_options().host,
                "DATABASE_URL changed at runtime, host differs"
            );

            assert_eq!(
                existing.connect_options().database,
                pool.connect_options().database,
                "DATABASE_URL changed at runtime, database differs"
            );

            existing
        }
    };

    let mut conn = master_pool.acquire().await?;

    // language=PostgreSQL
    conn.execute(
        // Explicit lock avoids this latent bug: https://stackoverflow.com/a/29908840
        // I couldn't find a bug on the mailing list for `CREATE SCHEMA` specifically,
        // but a clearly related bug with `CREATE TABLE` has been known since 2007:
        // https://www.postgresql.org/message-id/200710222037.l9MKbCJZ098744%40wwwmaster.postgresql.org
        // magic constant 8318549251334697844 is just 8 ascii bytes 'sqlxtest'.
        r#"
        select pg_advisory_xact_lock(8318549251334697844);

        create schema if not exists _sqlx_test;

        create table if not exists _sqlx_test.databases (
            db_name text primary key,
            test_path text not null,
            created_at timestamptz not null default now()
        );

        create index if not exists databases_created_at 
            on _sqlx_test.databases(created_at);

        create sequence if not exists _sqlx_test.database_ids;

        create table if not exists _sqlx_test.templates (
            db_name text primary key,
            created_at timestamptz not null default now(),
            last_used_at timestamptz not null default now()
        );

        alter table _sqlx_test.templates add column if not exists master_db text;
    "#,
    )
    .await?;

    let db_name = Postgres::db_name(args);
    do_cleanup(&mut conn, &db_name).await?;

    query(
        r#"
            insert into _sqlx_test.databases(db_name, test_path) values ($1, $2)
        "#,
    )
    .bind(&db_name)
    .bind(args.test_path)
    .execute(&mut *conn)
    .await?;

    let create_command = format!("create database {db_name:?}");
    debug_assert!(create_command.starts_with("create database \""));
    match args.migrator {
        Some(migrator) => {
            create_from_template(&mut conn, master_pool, migrator, &create_command).await?
        }
        None => {
            conn.execute(&(create_command)[..]).await?;
        }
    }

    Ok(TestContext {
        pool_opts: PoolOptions::new()
            // Don't allow a single test to take all the connections.
            // Most tests shouldn't require more than 5 connections concurrently,
            // or else they're likely doing too much in one test.
            .max_connections(5)
            // Close connections ASAP if left in the idle queue.
            .idle_timeout(Some(Duration::from_secs(1)))
            .parent(master_pool.clone()),
        connect_opts: master_pool
            .connect_options()
            .deref()
            .clone()
            .database(&db_name),
        db_name,
    })
}

/// Every test pool takes its connections out of the master pool's permits and keeps the ones it
/// took until it closes, so tests running at once can split all of them and each wait on a
/// connection another one holds until their acquire times out. `SQLX_TEST_MAX_CONNECTIONS` lifts
/// the cap: 5 per test thread, a test pool's own cap, rules that out.
fn master_max_connections() -> u32 {
    dotenvy::var("SQLX_TEST_MAX_CONNECTIONS")
        .ok()
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse()
                .expect("SQLX_TEST_MAX_CONNECTIONS must be a positive integer")
        })
        .unwrap_or(20)
}

async fn do_cleanup(conn: &mut PgConnection, db_name: &str) -> Result<(), Error> {
    let delete_db_command = format!("drop database if exists {db_name:?};");
    conn.execute(&*delete_db_command).await?;
    query("delete from _sqlx_test.databases where db_name = $1::text")
        .bind(db_name)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Runs `create_command` cloning a template that holds `migrator`'s migrations, building the
/// template first if needed.
///
/// `CREATE DATABASE .. TEMPLATE` is much cheaper than applying every migration per test. The
/// template's name hashes the master database and every migration, so a changed migration set
/// never reuses a stale template, and masters sharing a server never share one. Clones hold the
/// template's advisory lock shared and builds and drops hold it exclusively, so no template is
/// dropped or rebuilt under a clone. It counts as built only once its row exists and the
/// database is marked as a template, so one left by a crashed build is dropped and rebuilt.
async fn create_from_template(
    conn: &mut PgConnection,
    master_pool: &Pool<Postgres>,
    migrator: &Migrator,
    create_command: &str,
) -> Result<(), Error> {
    let master_opts = master_pool.connect_options();
    let master_db = master_opts.database.as_deref().unwrap_or_default();

    let mut hasher = Sha512::new();
    hasher.update(master_db.as_bytes());
    for migration in migrator.iter() {
        hasher.update(migration.version.to_le_bytes());
        hasher.update(format!("{:?}", migration.migration_type).as_bytes());
        hasher.update([migration.no_tx as u8]);
        hasher.update(migration.description.as_bytes());
        hasher.update(&*migration.checksum);
    }
    let hash = hasher.finalize();
    let template = format!("_sqlx_tmpl_{}", URL_SAFE.encode(&hash[..39])).replace('-', "_");
    debug_assert!(template.len() == 63);
    let lock_key = template_lock_key(&template);
    let clone_command = format!("{create_command} template {template:?}");

    // On error a held lock is released with the connection, which the master pool closes.
    query("select pg_advisory_lock_shared($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await?;
    if template_ready(conn, &template).await? {
        conn.execute(&*clone_command).await?;
        query("select pg_advisory_unlock_shared($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await?;
        return Ok(());
    }
    query("select pg_advisory_unlock_shared($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await?;

    query("select pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await?;
    if !template_ready(conn, &template).await? {
        build_template(conn, &master_opts, migrator, &template).await?;
        drop_stale_templates(conn, master_db, &template).await?;
    }
    conn.execute(&*clone_command).await?;
    query("select pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

fn template_lock_key(template: &str) -> i64 {
    let hash = Sha512::digest(template.as_bytes());
    i64::from_le_bytes(hash[..8].try_into().unwrap())
}

async fn template_ready(conn: &mut PgConnection, template: &str) -> Result<bool, Error> {
    Ok(query_scalar(
        "update _sqlx_test.templates set last_used_at = now() \
         where db_name = $1 \
         and exists (select from pg_database where datname = $1 and datistemplate) \
         returning true",
    )
    .bind(template)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(false))
}

async fn build_template(
    conn: &mut PgConnection,
    master_opts: &PgConnectOptions,
    migrator: &Migrator,
    template: &str,
) -> Result<(), Error> {
    // `force`: an interrupted build can leave its migration session running on the template.
    drop_template(conn, template, true).await?;
    conn.execute(&*format!("create database {template:?}"))
        .await?;

    let mut template_conn =
        PgConnection::connect_with(&master_opts.clone().database(template)).await?;
    migrator.run_direct(&mut template_conn).await?;
    template_conn.close().await?;

    // Nothing may stay connected to a template while it is cloned.
    conn.execute(&*format!(
        "alter database {template:?} with is_template true allow_connections false"
    ))
    .await?;
    query(
        "insert into _sqlx_test.templates(db_name, master_db) values ($1, current_database()) \
         on conflict (db_name) do update set created_at = now(), last_used_at = now()",
    )
    .bind(template)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Drops this master's templates of other migration sets once unused for a day, skipping any
/// a clone or build holds the lock of.
async fn drop_stale_templates(
    conn: &mut PgConnection,
    master_db: &str,
    current: &str,
) -> Result<(), Error> {
    // `master_db` keeps a master copied from another database from dropping that one's templates.
    let stale: Vec<String> = query_scalar(
        "select db_name from _sqlx_test.templates \
         where master_db = $1 and db_name <> $2 and last_used_at < now() - interval '1 day'",
    )
    .bind(master_db)
    .bind(current)
    .fetch_all(&mut *conn)
    .await?;

    for stale in stale {
        let lock_key = template_lock_key(&stale);
        let locked: bool = query_scalar("select pg_try_advisory_lock($1)")
            .bind(lock_key)
            .fetch_one(&mut *conn)
            .await?;
        if !locked {
            continue;
        }
        let still_stale: bool = query_scalar(
            "select exists (select from _sqlx_test.templates \
             where db_name = $1 and last_used_at < now() - interval '1 day')",
        )
        .bind(&stale)
        .fetch_one(&mut *conn)
        .await?;
        if still_stale {
            match drop_template(conn, &stale, false).await {
                Ok(()) => {
                    query("delete from _sqlx_test.templates where db_name = $1")
                        .bind(&stale)
                        .execute(&mut *conn)
                        .await?;
                }
                Err(Error::Database(dbe)) => {
                    eprintln!("could not drop test template {stale:?}: {dbe}")
                }
                Err(e) => return Err(e),
            }
        }
        query("select pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

async fn drop_template(conn: &mut PgConnection, template: &str, force: bool) -> Result<(), Error> {
    let exists: bool = query_scalar("select exists (select from pg_database where datname = $1)")
        .bind(template)
        .fetch_one(&mut *conn)
        .await?;
    if exists {
        conn.execute(&*format!(
            "alter database {template:?} with is_template false allow_connections true"
        ))
        .await?;
        let force = if force { " with (force)" } else { "" };
        conn.execute(&*format!("drop database {template:?}{force}"))
            .await?;
    }
    Ok(())
}
