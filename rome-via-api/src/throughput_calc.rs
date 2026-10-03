//! Pure peak-TPS window scan — no DB, no axum. The throughput "record".
//! Slides a window of `window_w` consecutive blocks (ascending by timestamp),
//! TPS = Σtx_count / (last.ts − first.ts), keeping the max where elapsed ≥ min_elapsed.

#[derive(Debug, Clone, Copy)]
pub struct BlockPoint {
    pub number: i64,
    pub ts: i64,      // Solana block time, whole seconds
    pub tx_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeakWindow {
    pub from_block: i64,
    pub to_block: i64,
    pub blocks: usize,
    pub total_txs: i64,
    pub elapsed_seconds: i64,
    pub tps: f64,
}

/// `points` MUST be ascending by `ts`. Returns the highest-TPS window of
/// `window_w` consecutive points whose elapsed ≥ `min_elapsed`, or None.
/// Ties resolve to the EARLIEST window (strict `>` on update).
pub fn peak_tps(points: &[BlockPoint], window_w: usize, min_elapsed: i64) -> Option<PeakWindow> {
    if window_w < 2 || points.len() < window_w {
        return None;
    }
    let mut best: Option<PeakWindow> = None;
    for start in 0..=(points.len() - window_w) {
        let win = &points[start..start + window_w];
        let first = win[0];
        let last = win[window_w - 1];
        let elapsed = last.ts - first.ts;
        if elapsed < min_elapsed {
            continue;
        }
        let total_txs: i64 = win.iter().map(|b| b.tx_count).sum();
        let tps = total_txs as f64 / elapsed as f64;
        let better = match &best {
            None => true,
            Some(b) => tps > b.tps,
        };
        if better {
            best = Some(PeakWindow {
                from_block: first.number,
                to_block: last.number,
                blocks: window_w,
                total_txs,
                elapsed_seconds: elapsed,
                tps,
            });
        }
    }
    best
}

/// `(slots_per_sec, avg_slot_ms)` from a slot-count delta over an elapsed-seconds delta.
/// Pure: the same math as the cluster's `getRecentPerformanceSamples` (slots ÷ period),
/// computed from indexed block `slot_number` + `params_block_timestamp` deltas. Guards
/// `time_span <= 0` / `slot_span <= 0` → `(0.0, 0.0)`.
pub fn slot_rate(slot_span: i64, time_span: i64) -> (f64, f64) {
    if time_span <= 0 || slot_span <= 0 {
        return (0.0, 0.0);
    }
    (
        slot_span as f64 / time_span as f64,
        time_span as f64 * 1000.0 / slot_span as f64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(number: i64, ts: i64, tx_count: i64) -> BlockPoint { BlockPoint { number, ts, tx_count } }

    #[test]
    fn slot_rate_computes_per_sec_and_ms() {
        // The measured devnet window: 600 slots over 226s ≈ 2.65/s, ~377 ms/slot.
        let (per_sec, ms) = slot_rate(600, 226);
        assert!((per_sec - 2.655).abs() < 0.01, "per_sec={per_sec}");
        assert!((ms - 376.7).abs() < 1.0, "ms={ms}");
    }

    #[test]
    fn slot_rate_guards_zero_and_negative_span() {
        assert_eq!(slot_rate(100, 0), (0.0, 0.0));
        assert_eq!(slot_rate(0, 100), (0.0, 0.0));
        assert_eq!(slot_rate(-5, 100), (0.0, 0.0));
    }

    #[test]
    fn finds_the_burst_window() {
        let pts = vec![p(100, 0, 1), p(101, 1, 1), p(102, 2, 10), p(103, 3, 30), p(104, 4, 2)];
        let w = peak_tps(&pts, 3, 1).unwrap();
        assert_eq!(w.from_block, 102);
        assert_eq!(w.to_block, 104);
        assert_eq!(w.total_txs, 42);
        assert_eq!(w.elapsed_seconds, 2);
        assert!((w.tps - 21.0).abs() < 1e-9);
    }

    #[test]
    fn excludes_windows_below_min_elapsed() {
        let pts = vec![p(1, 5, 100), p(2, 5, 100), p(3, 5, 100)];
        assert_eq!(peak_tps(&pts, 3, 1), None);
    }

    #[test]
    fn none_when_fewer_than_window() {
        let pts = vec![p(1, 0, 10), p(2, 1, 10)];
        assert_eq!(peak_tps(&pts, 3, 1), None);
    }

    #[test]
    fn all_empty_blocks_yield_zero_tps_window() {
        let pts = vec![p(1, 0, 0), p(2, 2, 0), p(3, 4, 0)];
        let w = peak_tps(&pts, 3, 1).unwrap();
        assert_eq!(w.total_txs, 0);
        assert!((w.tps - 0.0).abs() < 1e-9);
    }

    #[test]
    fn earliest_window_wins_on_tie() {
        let pts = vec![p(1, 0, 20), p(2, 1, 20), p(3, 2, 20), p(4, 3, 20), p(5, 4, 20)];
        let w = peak_tps(&pts, 2, 1).unwrap();
        assert_eq!(w.from_block, 1);
        assert_eq!(w.to_block, 2);
    }
}
