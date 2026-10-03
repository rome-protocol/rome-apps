//! Proves the disposable-per-test-DB harness actually tears its database
//! down — the CI leak fix's anchor test (rome-apps `unit-test` CI hang from
//! leaked databases starving the shared Postgres, PR #537).
//!
//! RED-then-GREEN: before `common::TestDb` grew a `Drop` impl, an equivalent
//! probe against the pre-fix code (plain `CREATE DATABASE` + a `PgPool` with
//! nothing tearing it down — see the fix commit) failed with exactly the
//! assertion below. It now passes because scope-exit of the guard returned
//! by `common::fresh_empty_db()` runs `DROP DATABASE`.

mod common;

use sqlx::PgPool;

async fn database_exists(admin_pool: &PgPool, db_name: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(db_name)
        .fetch_one(admin_pool)
        .await
        .unwrap()
}

/// The anchor test: create a disposable test DB via the shared harness,
/// capture its name, drop the guard (end its scope), then assert — against
/// a SEPARATE admin connection — that the database is actually gone.
#[tokio::test]
async fn dropping_the_guard_drops_the_database() {
    let admin_pool = PgPool::connect(&common::admin_url())
        .await
        .expect("connect to the test Postgres admin DB");

    let db_name = {
        let db = common::fresh_empty_db().await;
        let name = db.name().to_string();
        assert!(
            database_exists(&admin_pool, &name).await,
            "setup sanity: the freshly created database must exist"
        );
        name
        // `db` (the guard) drops here, at the end of this block.
    };

    assert!(
        !database_exists(&admin_pool, &db_name).await,
        "database {db_name} must have been dropped when its guard went out of scope"
    );
}

/// A normal DB-backed test must still get a fully usable pool — the
/// teardown mechanism must not perturb the setup path other tests rely on.
#[tokio::test]
async fn a_normal_db_backed_test_still_gets_a_usable_pool() {
    let pool = common::fresh_audit_db().await;
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM audit.chain_event")
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0, "a freshly migrated audit DB starts empty");
}
