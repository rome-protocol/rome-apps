//! Guard: sqlx keys migrations by numeric version prefix — two files
//! sharing a prefix poison a deployed DB (matches rome-via-enrich's
//! identical guard, `rome-via-enrich/src/migrations_test.rs`).
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn migration_version_prefixes_are_unique() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
        let mut seen: HashMap<String, String> = HashMap::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if !name.ends_with(".up.sql") {
                continue;
            }
            let version = name.split('_').next().unwrap().to_string();
            if let Some(prev) = seen.insert(version.clone(), name.clone()) {
                panic!("duplicate migration version {version}: {prev} and {name}");
            }
        }
        assert!(!seen.is_empty(), "no migrations found — wrong dir?");
    }

    /// rome-audit OWNS the `[900, 999]` version band in the shared
    /// `rome_via_db` — rome-via-sync claims `0001-0019`, rome-via-enrich
    /// `0100+`. A migration numbered outside 900-999 would collide with a
    /// co-tenant's band on the shared DB (the #528 renumber that produced
    /// 0901-0910 existed precisely to escape that collision). This makes the
    /// band invariant mechanical rather than a convention someone has to
    /// remember.
    #[test]
    fn migration_versions_are_in_the_rome_audit_band() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
        let mut count = 0usize;
        for entry in std::fs::read_dir(dir).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if !name.ends_with(".up.sql") {
                continue;
            }
            let prefix = name.split('_').next().unwrap();
            let version: u32 = prefix
                .parse()
                .unwrap_or_else(|_| panic!("migration {name} has a non-numeric version prefix {prefix:?}"));
            assert!(
                (900..=999).contains(&version),
                "migration {name} (version {version}) is outside rome-audit's [900, 999] band \
                 in the shared rome_via_db (sync=0001-0019, enrich=0100+)"
            );
            count += 1;
        }
        assert!(count > 0, "no migrations found — wrong dir?");
    }

    /// P5a test #1 — migration 0910 (Tier-3 overlay) applies cleanly, its
    /// `.down.sql` reverts cleanly, and a second `up` re-applies cleanly.
    /// Runs against the real disposable Postgres container the rest of the
    /// suite uses (`ROME_AUDIT_TEST_PG_ADMIN_URL` override, default
    /// `rome-audit-test-pg` on :55432) — a self-contained fresh DB, not
    /// `tests/common`'s helper (that module is integration-test-only; this
    /// is a `--lib` unit test).
    #[tokio::test]
    async fn migration_0910_up_down_up() {
        let admin_url = std::env::var("ROME_AUDIT_TEST_PG_ADMIN_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:55432/postgres".to_string());
        let admin_pool = sqlx::PgPool::connect(&admin_url)
            .await
            .expect("connect to the test Postgres admin DB — is rome-audit-test-pg running?");

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_name = format!("rome_audit_migtest_{}_{}", std::process::id(), nanos);
        sqlx::query(&format!("CREATE DATABASE {db_name}"))
            .execute(&admin_pool)
            .await
            .expect("CREATE DATABASE for a fresh isolated test DB");

        let db_url = format!("{}/{db_name}", admin_url.trim_end_matches("/postgres"));
        let pool = sqlx::PgPool::connect(&db_url)
            .await
            .expect("connect to freshly created test DB");

        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
        let migrator = sqlx::migrate::Migrator::new(std::path::Path::new(dir))
            .await
            .expect("load migrator");

        // up (to and including 0910)
        migrator.run(&pool).await.expect("full up-migration, incl. 0910, must apply");

        for expect in ["identity", "identity_key", "evidence_record", "address_label"] {
            let (exists,): (bool,) = sqlx::query_as(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='audit' AND table_name=$1)",
            )
            .bind(expect)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(exists, "audit.{expect} must exist after up-migration");
        }

        // down (revert exactly 0910, back to 0909)
        migrator.undo(&pool, 909).await.expect("0910's down-migration must revert cleanly");

        for gone in ["identity", "identity_key", "evidence_record", "address_label"] {
            let (exists,): (bool,) = sqlx::query_as(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='audit' AND table_name=$1)",
            )
            .bind(gone)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(!exists, "audit.{gone} must be gone after reverting 0910");
        }

        // up again (re-apply 0910) — must succeed a second time.
        migrator.run(&pool).await.expect("re-applying 0910 after a clean revert must succeed");

        let (exists,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='audit' AND table_name='identity')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "audit.identity must exist again after the second up");

        pool.close().await;
        sqlx::query(&format!("DROP DATABASE {db_name}"))
            .execute(&admin_pool)
            .await
            .ok();
    }
}
