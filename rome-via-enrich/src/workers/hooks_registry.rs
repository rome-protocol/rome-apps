/// Hooks registry worker.
///
/// Currently populates `rome_via.hooks_registry` from the set of (hook_address,
/// hook_kind) pairs observed by the `hook_executions` worker. Keeps the
/// `/api/v1/hooks` endpoint in sync with what has actually fired on chain —
/// no hard-coded KYC placeholder.
///
/// Each run also deletes the stale `HookKycProgram1111…` seed that an earlier
/// version of this worker inserted.
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, warn};

pub async fn run(pool: PgPool, chain_id: i64) -> anyhow::Result<()> {
    // Purge legacy static seed (idempotent — no-op after first run).
    if let Err(e) = sqlx::query(
        "DELETE FROM rome_via.hooks_registry
         WHERE chain_id = $1
           AND hook_address = 'HookKycProgram1111111111111111111111111111'",
    )
    .bind(chain_id)
    .execute(&pool)
    .await
    {
        warn!(error = %e, "failed to purge legacy KYC registry entry");
    }

    loop {
        // Reflect observed hooks into the registry. `token_address` stays
        // NULL until we wire log-tailing of router registration events.
        let result = sqlx::query(
            r#"
            INSERT INTO rome_via.hooks_registry
                (chain_id, token_address, hook_address, hook_kind, registered_at, source)
            SELECT DISTINCT
                he.chain_id,
                COALESCE('', ''),
                he.hook_address,
                he.hook_kind,
                NOW(),
                'observed'
            FROM rome_via.hook_executions he
            WHERE he.chain_id = $1
            ON CONFLICT (chain_id, token_address, hook_address) DO NOTHING
            "#,
        )
        .bind(chain_id)
        .execute(&pool)
        .await;

        match result {
            Ok(r) if r.rows_affected() > 0 => {
                debug!(
                    added = r.rows_affected(),
                    "hooks_registry refreshed from observed executions"
                );
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "hooks_registry refresh failed"),
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
