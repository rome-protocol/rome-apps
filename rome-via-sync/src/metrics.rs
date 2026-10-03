use rome_obs::meter::OtelMeter;
use rome_obs::KeyValue;

/// Record replication lag for a given table + chain_id.
///
/// Gauge: `rome_via_sync_replication_lag_seconds{table, chain_id}`.
/// Value is the number of whole seconds since last_synced_at.
pub async fn record_lag(table: &'static str, chain_id: u64, lag_seconds: u64) {
    if let Some(meter) = OtelMeter::get() {
        let attrs = vec![
            KeyValue::new("table", table),
            KeyValue::new("chain_id", chain_id.to_string()),
        ];
        let _ = meter
            .record(
                "rome_via_sync_replication_lag_seconds",
                lag_seconds,
                Some(attrs),
            )
            .await;
    }
}

/// Increment rows-synced counter for a given table + chain_id.
///
/// Counter: `rome_via_sync_rows_synced_total{table, chain_id}`.
pub fn inc_rows_synced(table: &'static str, chain_id: u64) {
    if let Some(meter) = OtelMeter::get() {
        let attrs = [
            KeyValue::new("table", table),
            KeyValue::new("chain_id", chain_id.to_string()),
        ];
        meter.count(
            "rome_via_sync_rows_synced_total".to_string(),
            Some(&attrs),
        );
    }
}

/// Increment the RLP decode error counter for a given chain_id.
///
/// Counter: `rome_via_sync_rlp_decode_errors_total{chain_id}`.
/// Emitted when `decode_signed_tx` fails for an `evm_tx` row.
/// The row is still inserted with NULL decoded fields — sync is not blocked.
pub fn inc_rlp_decode_errors(chain_id: u64) {
    if let Some(meter) = OtelMeter::get() {
        let attrs = [KeyValue::new("chain_id", chain_id.to_string())];
        meter.count(
            "rome_via_sync_rlp_decode_errors_total".to_string(),
            Some(&attrs),
        );
    }
}
