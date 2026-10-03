//! Pure top-K throughput record: deduped sustained-TPS windows + busiest blocks.
/// Depth of both persisted top-K lists.
///
/// 100, not 10: /throughput/top-blocks defaults to limit=25, so a 10-deep record could
/// never serve a default request and the endpoint fell through to a 5-8s live scan
/// every time. 100 covers the endpoint's max limit too, and the rows are tiny.
pub const TOP_K: usize = 100;

/// Most rows of one block number the window pool is sized for (`rome_via.eth_block` is keyed by
/// (chain_id, slot_number, slot_block_idx), so `params_number` can repeat). Each extra duplicate
/// of a block number lets a kept window block two more windows by block-number range.
const DUP_MARGIN: usize = 4;

#[derive(Clone, Copy)]
pub struct Point {
    pub block: i64,
    pub slot: i64,
    pub ts: i64,
    pub total_txs: i64,
    pub app_txs: i64,
    /// Block gas used — carried so the record can serve TopBlock.gas_used rather than
    /// emitting a placeholder that silently differs from the live scan.
    pub gas_used: i64,
}

#[derive(Clone)]
pub struct WindowRec {
    pub from_block: i64,
    pub to_block: i64,
    pub from_slot: i64,
    pub to_slot: i64,
    pub elapsed_seconds: i64,
    pub total_txs: i64,
    pub app_txs: i64,
    pub total_tps: f64,
    pub app_tps: f64,
}

#[derive(Clone)]
pub struct BlockRec {
    pub block: i64,
    pub slot: i64,
    pub total_txs: i64,
    pub app_txs: i64,
    pub ts: i64,
    pub gas_used: i64,
}

pub struct Record {
    pub windows: Vec<WindowRec>,
    pub blocks: Vec<BlockRec>,
    /// Block-size distribution over every block scanned, counts per `HIST_LABELS` bucket.
    /// Persisted so `/throughput/histogram?range=all` is a table read instead of a
    /// per-block CTE over the whole chain (measured 18.7s under load).
    pub hist: Hist,
}

/// Bucket labels for the block-tx-count histogram. Must stay in lockstep with the
/// buckets rome-via-api's live-scan fallback emits, so both paths render identically.
pub const HIST_LABELS: [&str; 6] = ["0", "1", "2-5", "6-20", "21-50", "50+"];
pub type Hist = [i64; 6];

/// Bucket index for a block's total tx count. Boundaries land in the LOWER bucket
/// (5→"2-5", 20→"6-20", 50→"21-50"), matching the API's FILTER predicates.
pub fn bucket_of(total_txs: i64) -> usize {
    match total_txs {
        n if n <= 0 => 0,
        1 => 1,
        2..=5 => 2,
        6..=20 => 3,
        21..=50 => 4,
        _ => 5,
    }
}

/// Histogram over the given points.
pub fn histogram_of(points: &[Point]) -> Hist {
    let mut h: Hist = [0; 6];
    for p in points {
        h[bucket_of(p.total_txs)] += 1;
    }
    h
}

/// `points` MUST be ascending by block. Builds both top-10 lists from scratch (seed / backstop).
pub fn compute_record(points: &[Point], w: usize, min_elapsed: i64) -> Record {
    let blocks = topk_blocks(
        points
            .iter()
            .map(|p| BlockRec {
                block: p.block,
                slot: p.slot,
                total_txs: p.total_txs,
                app_txs: p.app_txs,
                ts: p.ts,
                gas_used: p.gas_used,
            })
            .collect(),
    );
    let windows = topk_windows(windows_in(points, w, min_elapsed));
    Record { windows, blocks, hist: histogram_of(points) }
}

/// All valid W-length windows over ascending `points` (no ranking / dedup).
pub fn windows_in(points: &[Point], w: usize, min_elapsed: i64) -> Vec<WindowRec> {
    let mut out = Vec::new();
    if w < 2 || points.len() < w {
        return out;
    }
    for start in 0..=(points.len() - w) {
        let win = &points[start..start + w];
        let elapsed = win[w - 1].ts - win[0].ts;
        if elapsed < min_elapsed {
            continue;
        }
        let total: i64 = win.iter().map(|p| p.total_txs).sum();
        let app: i64 = win.iter().map(|p| p.app_txs).sum();
        out.push(WindowRec {
            from_block: win[0].block,
            to_block: win[w - 1].block,
            from_slot: win[0].slot,
            to_slot: win[w - 1].slot,
            elapsed_seconds: elapsed,
            total_txs: total,
            app_txs: app,
            total_tps: total as f64 / elapsed as f64,
            app_tps: app as f64 / elapsed as f64,
        });
    }
    out
}

/// Rank windows by app_tps desc, greedy non-overlap, keep TOP_K.
fn topk_windows(mut all: Vec<WindowRec>) -> Vec<WindowRec> {
    all.sort_by(cmp_windows);
    let mut kept: Vec<WindowRec> = Vec::new();
    for cand in all {
        let overlaps = kept
            .iter()
            .any(|k| !(cand.to_block < k.from_block || k.to_block < cand.from_block));
        if !overlaps {
            kept.push(cand);
        }
        if kept.len() == TOP_K {
            break;
        }
    }
    kept
}

/// Rank blocks by app_txs desc (block asc tie-break), keep TOP_K.
/// Callers pass unique block numbers (SQL `GROUP BY block`; merge feeds a disjoint range), so no dedup.
fn topk_blocks(mut all: Vec<BlockRec>) -> Vec<BlockRec> {
    all.sort_by(|a, b| b.app_txs.cmp(&a.app_txs).then(a.block.cmp(&b.block)));
    all.truncate(TOP_K);
    all
}

/// Incremental merge: fold blocks/windows NEW since `cursor_block` into an existing top-K record.
/// `overlap_points` MUST include the W-1 points before the first new block so boundary windows form.
/// Correct for append-only growth; the worker's periodic full recompute self-heals any historical
/// change (e.g. a backfill) — spec §6.
pub fn merge_record(
    existing: &Record,
    overlap_points: &[Point],
    cursor_block: i64,
    w: usize,
    min_elapsed: i64,
) -> Record {
    let mut new_windows = windows_in(overlap_points, w, min_elapsed);
    new_windows.retain(|win| win.to_block > cursor_block); // older windows were ranked already
    let mut cand_w = existing.windows.clone();
    cand_w.append(&mut new_windows);
    let windows = topk_windows(cand_w);

    let mut cand_b = existing.blocks.clone(); // existing blocks all <= cursor → disjoint
    cand_b.extend(
        overlap_points
            .iter()
            .filter(|p| p.block > cursor_block)
            .map(|p| BlockRec {
                block: p.block,
                slot: p.slot,
                total_txs: p.total_txs,
                app_txs: p.app_txs,
                ts: p.ts,
                gas_used: p.gas_used,
            }),
    );
    let blocks = topk_blocks(cand_b);

    // Histogram is cumulative over every block ever scanned, so fold in ONLY the blocks
    // past the cursor — the W-1 overlap points were already counted by the previous pass.
    let mut hist = existing.hist;
    let new_only: Vec<Point> = overlap_points.iter().filter(|p| p.block > cursor_block).copied().collect();
    let delta = histogram_of(&new_only);
    for (h, d) in hist.iter_mut().zip(delta.iter()) {
        *h += d;
    }

    Record { windows, blocks, hist }
}

/// Make a page end on a complete block number. `rome_via.eth_block` is keyed by
/// (chain_id, slot_number, slot_block_idx), so `params_number` can repeat across rows and a
/// LIMIT can cut between two rows of one block. When the page is full (`len == page_size`) the
/// rows of its last block number are dropped, so the cursor (the largest remaining block number)
/// makes the next page re-read that block whole. A page that is not full reached the end of the
/// data and is already complete.
///
/// A full page holding a single block number is NOT trimmed: dropping it would leave an empty
/// page, the cursor would never advance and the caller would loop forever. Such a page counts as
/// complete (a block with `page_size` rows is not a real chain shape).
pub fn trim_full_page(page: &mut Vec<Point>, page_size: usize) {
    if page.len() < page_size {
        return;
    }
    let (Some(first), Some(last)) = (page.first().map(|p| p.block), page.last().map(|p| p.block)) else {
        return;
    };
    if first == last {
        tracing::warn!(
            block = first,
            page_size,
            "a full page holds one block number; keeping it whole so the cursor advances"
        );
        return;
    }
    while page.last().is_some_and(|p| p.block == last) {
        page.pop();
    }
}

/// True when a fetched page of `len` rows filled `page_size`, i.e. more rows may follow it.
/// A shorter page reached the end of the data.
pub fn page_is_full(len: usize, page_size: usize) -> bool {
    len >= page_size
}

/// True when a truncated window pool may have cost the record windows: the pool dropped
/// candidates at some point and the final selection still came up short of `TOP_K`.
fn window_record_may_be_short(pool_truncated: bool, selected: usize) -> bool {
    pool_truncated && selected < TOP_K
}

/// Orders windows best first: app_tps desc, then from_block asc.
fn cmp_windows(a: &WindowRec, b: &WindowRec) -> std::cmp::Ordering {
    b.app_tps.partial_cmp(&a.app_tps).unwrap().then(a.from_block.cmp(&b.from_block))
}

/// Paged full rebuild (seed / backstop). Feed it pages of ascending points with
/// `push_page`; memory stays proportional to one page plus a fixed-size state, and
/// `finish` yields the same `Record` (and cursor) `compute_record` over every point would.
///
/// The ranked window list is not folded with `merge_record`: that greedy-filters to a
/// non-overlapping top-K after every page, and a window kept early can later be displaced by one
/// overlapping a page boundary, which would have un-blocked a neighbour already discarded. Instead
/// the state keeps the best `TOP_K * (2w - 1 + 2(D - 1))` raw windows, D = `DUP_MARGIN`. Windows
/// overlap by block-number range, and a kept window blocks at most 2w-2 others when every block
/// number appears once, 2w-2+2(D-1) when a block number can appear up to D times. Greedy selection
/// of TOP_K disjoint windows never looks deeper than that, so the final greedy pass in `finish` is
/// exact for block numbers repeated at most D times. Beyond that the pool can run short: `finish`
/// logs a warning when the pool was ever truncated and fewer than TOP_K windows came out.
pub struct PagedRebuild {
    w: usize,
    min_elapsed: i64,
    /// Best raw windows so far, sorted best first, capped at `pool_cap`.
    pool: Vec<WindowRec>,
    pool_cap: usize,
    /// Set once the pool dropped candidates because of `pool_cap`.
    pool_truncated: bool,
    blocks: Vec<BlockRec>,
    hist: Hist,
    /// The last w-1 points folded so far: the overlap that lets boundary windows form.
    tail: Vec<Point>,
    cursor: Option<i64>,
}

impl PagedRebuild {
    pub fn new(w: usize, min_elapsed: i64) -> Self {
        Self {
            w,
            min_elapsed,
            pool: Vec::new(),
            pool_cap: TOP_K * (2 * w.max(2) - 1 + 2 * (DUP_MARGIN - 1)),
            pool_truncated: false,
            blocks: Vec::new(),
            hist: [0; 6],
            tail: Vec::new(),
            cursor: None,
        }
    }

    /// Block number to fetch after (`i64::MIN` before the first page).
    pub fn cursor(&self) -> i64 {
        self.cursor.unwrap_or(i64::MIN)
    }

    /// Fold one fetched page (ascending, every block above `cursor()`). Returns whether to fetch
    /// another page: false after an empty page and after a page shorter than `page_size`, which
    /// reached the end of the data (the page is still folded).
    pub fn push_page(&mut self, mut page: Vec<Point>, page_size: usize) -> bool {
        if page.is_empty() {
            return false;
        }
        let was_full = page_is_full(page.len(), page_size);
        trim_full_page(&mut page, page_size);
        let cursor = self.cursor();
        debug_assert!(page.iter().all(|p| p.block > cursor), "page must lie above the cursor");

        let mut combined = std::mem::take(&mut self.tail);
        combined.extend_from_slice(&page);
        let mut fresh = windows_in(&combined, self.w, self.min_elapsed);
        fresh.retain(|win| win.to_block > cursor); // older windows were ranked in an earlier page
        self.pool.append(&mut fresh);
        self.pool.sort_by(cmp_windows);
        if self.pool.len() > self.pool_cap {
            self.pool_truncated = true;
            self.pool.truncate(self.pool_cap);
        }

        self.blocks.extend(page.iter().map(|p| BlockRec {
            block: p.block,
            slot: p.slot,
            ts: p.ts,
            total_txs: p.total_txs,
            app_txs: p.app_txs,
            gas_used: p.gas_used,
        }));
        self.blocks = topk_blocks(std::mem::take(&mut self.blocks));
        for (h, d) in self.hist.iter_mut().zip(histogram_of(&page)) {
            *h += d;
        }

        let keep_from = combined.len().saturating_sub(self.w.saturating_sub(1));
        self.tail = combined.split_off(keep_from);
        self.cursor = page.last().map(|p| p.block);
        was_full
    }

    /// The finished record and the cursor to persist (`None` when no block was seen).
    pub fn finish(self) -> (Record, Option<i64>) {
        let windows = topk_windows(self.pool);
        if window_record_may_be_short(self.pool_truncated, windows.len()) {
            tracing::warn!(
                selected = windows.len(),
                top_k = TOP_K,
                "window pool was truncated and fewer than TOP_K windows were selected; the record may be short (block numbers repeated more than {DUP_MARGIN} times?)"
            );
        }
        let record = Record { windows, blocks: self.blocks, hist: self.hist };
        (record, self.cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(block: i64, ts: i64, total: i64, app: i64) -> Point {
        Point { block, slot: block, ts, total_txs: total, app_txs: app, gas_used: 0 }
    }
    fn pt_gas(block: i64, ts: i64, total: i64, app: i64, gas_used: i64) -> Point {
        Point { block, slot: block, ts, total_txs: total, app_txs: app, gas_used }
    }

    #[test]
    fn picks_top_window_by_app_tps_excluding_oracle() {
        // burst of 10 blocks @1s apart, 600 app txs + 50 oracle each → app_tps high.
        let pts: Vec<Point> = (0..12).map(|i| pt(100 + i, 1000 + i, 650, 600)).collect();
        let r = compute_record(&pts, 10, 1);
        let w = &r.windows[0];
        assert_eq!((w.from_block, w.to_block), (100, 109));
        assert_eq!(w.app_txs, 6000);          // 10 * 600, oracle excluded from app
        assert!((w.app_tps - 6000.0 / 9.0).abs() < 1e-6); // elapsed = ts[109]-ts[100] = 9
    }

    #[test]
    fn dedups_overlapping_windows_to_distinct_bursts() {
        // one long 20-block burst → top-10 must be 10 NON-overlapping windows, not 10 shifts of one.
        let pts: Vec<Point> = (0..30).map(|i| pt(i, i, 100, 100)).collect();
        let r = compute_record(&pts, 10, 1);
        for a in 0..r.windows.len() {
            for b in (a + 1)..r.windows.len() {
                let (x, y) = (&r.windows[a], &r.windows[b]);
                assert!(
                    x.to_block < y.from_block || y.to_block < x.from_block,
                    "windows {a} and {b} overlap"
                );
            }
        }
    }

    #[test]
    fn excludes_windows_below_min_elapsed() {
        let pts: Vec<Point> = (0..10).map(|i| pt(i, 0, 100, 100)).collect(); // all ts=0 → elapsed 0
        let r = compute_record(&pts, 10, 1);
        assert!(r.windows.is_empty());
    }

    #[test]
    fn busiest_blocks_ranked_by_app_txs() {
        let pts = vec![pt(1, 1, 900, 100), pt(2, 2, 500, 500), pt(3, 3, 800, 300)];
        let r = compute_record(&pts, 10, 1);
        assert_eq!(r.blocks[0].block, 2);  // app_txs 500 wins, not block 1's total 900
        assert_eq!(r.blocks[0].app_txs, 500);
    }

    /// The busiest-blocks record must be able to SERVE /throughput/top-blocks, whose
    /// default limit is 25. A 10-deep record silently can't, so the endpoint would keep
    /// falling through to the 5-8s live scan for every default request.
    #[test]
    fn top_k_covers_the_top_blocks_endpoint_default_limit() {
        const TOP_BLOCKS_DEFAULT_LIMIT: usize = 25;
        assert!(
            TOP_K >= TOP_BLOCKS_DEFAULT_LIMIT,
            "TOP_K={TOP_K} cannot serve a default page of {TOP_BLOCKS_DEFAULT_LIMIT}"
        );
    }

    /// TopBlock exposes gas_used, so the record has to carry it or the fast path would
    /// have to lie (emit \"0\") and silently differ from the live scan.
    #[test]
    fn busiest_blocks_carry_gas_used_so_the_fast_path_matches_the_live_scan() {
        let pts = vec![pt_gas(1, 1, 900, 100, 1_234_567), pt_gas(2, 2, 500, 500, 7_654_321)];
        let r = compute_record(&pts, 10, 1);
        assert_eq!(r.blocks[0].block, 2);
        assert_eq!(r.blocks[0].gas_used, 7_654_321, "gas must survive into the record");
    }

    #[test]
    fn histogram_buckets_blocks_by_total_tx_count() {
        // one block in each bucket boundary: 0, 1, 2-5, 6-20, 21-50, 50+
        let pts = vec![
            pt(1, 1, 0, 0), pt(2, 2, 1, 1), pt(3, 3, 5, 5),
            pt(4, 4, 20, 20), pt(5, 5, 50, 50), pt(6, 6, 51, 51),
        ];
        let r = compute_record(&pts, 10, 1);
        assert_eq!(r.hist, [1, 1, 1, 1, 1, 1], "one block per bucket");
        // boundaries land in the LOWER bucket: 5→"2-5", 20→"6-20", 50→"21-50"
        let edges = vec![pt(1, 1, 2, 2), pt(2, 2, 6, 6), pt(3, 3, 21, 21)];
        assert_eq!(compute_record(&edges, 10, 1).hist, [0, 0, 1, 1, 1, 0]);
    }

    #[test]
    fn incremental_merge_accumulates_histogram_without_double_counting_overlap() {
        // The merge overlap re-fetches W-1 already-counted blocks; they must NOT be re-added.
        let all: Vec<Point> = (0..40).map(|i| pt(i, i, 3, 3)).collect(); // every block in "2-5"
        let split = 20;
        let cursor = all[split - 1].block; // 19
        let seed = compute_record(&all[..split], 10, 1);
        assert_eq!(seed.hist[2], 20);
        let overlap = &all[(cursor as usize) - 9..]; // W-1 overlap + new blocks
        let merged = merge_record(&seed, overlap, cursor, 10, 1);
        let full = compute_record(&all, 10, 1);
        assert_eq!(merged.hist, full.hist, "merge must equal a full recompute");
        assert_eq!(merged.hist[2], 40, "40 blocks, none double-counted");
    }

    #[test]
    fn incremental_merge_reaches_global_peak_for_append() {
        // seed over an early region, then fold the rest in incrementally; the global-max burst
        // (blocks 28..37) is in the NEW region and must surface as rank-1, same as a full recompute.
        let mk = |i: i64| { let load = if (28..=37).contains(&i) { 900 } else { 100 }; pt(i, i, load, load) };
        let all: Vec<Point> = (0..40).map(mk).collect();
        let split = 20;                                  // seed over blocks 0..19
        let cursor = all[split - 1].block;               // 19
        let seed = compute_record(&all[..split], 10, 1);
        let overlap = &all[(cursor as usize) - 9..];     // W-1 overlap + all new blocks (10..39)
        let merged = merge_record(&seed, overlap, cursor, 10, 1);
        let full = compute_record(&all, 10, 1);
        assert_eq!(merged.windows[0].to_block, full.windows[0].to_block); // = 37
        assert!((merged.windows[0].app_tps - full.windows[0].app_tps).abs() < 1e-6);
        assert_eq!(merged.blocks[0].app_txs, full.blocks[0].app_txs);     // = 900
        for a in 0..merged.windows.len() {               // non-overlap invariant survives the merge
            for b in (a + 1)..merged.windows.len() {
                let (x, y) = (&merged.windows[a], &merged.windows[b]);
                assert!(x.to_block < y.from_block || y.to_block < x.from_block);
            }
        }
    }
}


#[cfg(test)]
mod paged_tests {
    use super::*;

    fn p(block: i64, slot: i64, ts: i64, total: i64, app: i64) -> Point {
        Point { block, slot, ts, total_txs: total, app_txs: app, gas_used: block }
    }

    /// Deterministic pseudo-random loads so windows and busiest blocks have real ranking contests.
    fn lcg(seed: &mut u64) -> i64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((*seed >> 33) % 1000) as i64
    }

    /// Contiguous chain: block i at slot i, ts advancing 1 or 2 s.
    fn chain(n: i64) -> Vec<Point> {
        let mut seed = 7u64;
        let mut ts = 1000;
        (0..n)
            .map(|i| {
                ts += 1 + lcg(&mut seed) % 2;
                let total = lcg(&mut seed) % 60;
                let app = total - lcg(&mut seed) % (total + 1);
                p(i, i, ts, total, app)
            })
            .collect()
    }

    /// Same, but block numbers repeat on a different slot every `every` blocks
    /// (params_number is not unique; the PK is chain, slot, slot_block_idx).
    fn chain_with_duplicates(n: i64, every: i64) -> Vec<Point> {
        let mut out = Vec::new();
        for pt in chain(n) {
            out.push(pt);
            if pt.block % every == 0 {
                out.push(Point { slot: pt.slot + 1000, total_txs: pt.total_txs + 3, ..pt });
            }
        }
        out
    }

    /// What the SQL page query does: rows above the cursor, ascending, at most `page_size`.
    fn fake_fetch(all: &[Point], after: i64, page_size: usize) -> Vec<Point> {
        all.iter().filter(|x| x.block > after).take(page_size).copied().collect()
    }

    /// Returns (record, cursor, pages folded, fetches issued).
    fn rebuild_counting(all: &[Point], page_size: usize, w: usize) -> (Record, Option<i64>, usize, usize) {
        let mut r = PagedRebuild::new(w, 1);
        let (mut pages, mut fetches) = (0, 0);
        loop {
            let page = fake_fetch(all, r.cursor(), page_size);
            fetches += 1;
            if !page.is_empty() {
                pages += 1;
            }
            if !r.push_page(page, page_size) {
                break;
            }
            assert!(fetches < 100_000, "rebuild must make progress");
        }
        let (rec, cur) = r.finish();
        (rec, cur, pages, fetches)
    }

    fn rebuild(all: &[Point], page_size: usize, w: usize) -> (Record, Option<i64>, usize) {
        let (rec, cur, pages, _) = rebuild_counting(all, page_size, w);
        (rec, cur, pages)
    }

    fn assert_same(a: &Record, b: &Record, what: &str) {
        let win = |r: &Record| -> Vec<_> {
            r.windows
                .iter()
                .map(|w| (w.from_block, w.to_block, w.from_slot, w.to_slot, w.elapsed_seconds, w.total_txs, w.app_txs))
                .collect()
        };
        let blk = |r: &Record| -> Vec<_> {
            r.blocks.iter().map(|x| (x.block, x.slot, x.total_txs, x.app_txs, x.ts, x.gas_used)).collect()
        };
        assert_eq!(win(a), win(b), "windows differ: {what}");
        assert_eq!(blk(a), blk(b), "busiest blocks differ: {what}");
        assert_eq!(a.hist, b.hist, "histogram differs: {what}");
    }

    /// w=2 clusters of rows A(b), B(b+1), B'(b+1 on another slot), C(b+2). Repeated block
    /// numbers make one kept window (AB) block three others (BB', B'C and the bridge before it),
    /// more than the 2w-2 the pool was sized for. The 75 strongest clusters fill a 3-per-kept
    /// pool with all four of their windows; greedy over that pool stops at 76, while the whole
    /// list (compute_record) reaches TOP_K because the 45 weaker clusters still have an AB each.
    fn duplicate_clusters(clusters: i64, strong: i64) -> Vec<Point> {
        let mut out = Vec::new();
        for c in 0..clusters {
            let (b, t) = (10 * c, 6 * c);
            let m = if c < strong { 1000 } else { 1 };
            // AB = 15m > B'C = 11m > BB' = 10m; the bridge to the next cluster has a 3s gap.
            out.push(p(b, b, t, 10 * m, 10 * m));
            out.push(p(b + 1, b + 1, t + 1, 5 * m, 5 * m));
            out.push(p(b + 1, b + 1001, t + 2, 5 * m, 5 * m));
            out.push(p(b + 2, b + 2, t + 3, 6 * m, 6 * m));
        }
        out
    }

    #[test]
    fn repeated_block_numbers_do_not_shrink_the_paged_window_record() {
        let all = duplicate_clusters(120, 75);
        let full = compute_record(&all, 2, 1);
        assert_eq!(full.windows.len(), TOP_K, "the full computation fills the record");
        let (rec, _, pages) = rebuild(&all, 10_000, 2);
        assert_eq!(pages, 1, "one page holds the whole chain");
        assert_eq!(rec.windows.len(), TOP_K, "paged rebuild fell short of TOP_K");
        assert_same(&rec, &full, "w=2 duplicate-block clusters");
    }

    #[test]
    fn page_fullness_and_short_record_detection_are_exact() {
        assert!(page_is_full(7, 7) && page_is_full(8, 7) && !page_is_full(6, 7) && !page_is_full(0, 7));
        assert!(window_record_may_be_short(true, TOP_K - 1));
        assert!(!window_record_may_be_short(true, TOP_K), "a full record is exact even after truncation");
        assert!(!window_record_may_be_short(false, 3), "an untruncated pool is the whole list");
    }

    #[test]
    fn a_final_partial_page_ends_the_rebuild_without_another_fetch() {
        let all = chain(20);
        for page_size in [7usize, 9, 19, 50] {
            let (_, cursor, pages, fetches) = rebuild_counting(&all, page_size, 10);
            assert_eq!(cursor, Some(19), "page_size={page_size}");
            assert_eq!(fetches, pages, "page_size={page_size}: a fetch after the partial page is wasted");
        }
    }

    #[test]
    fn paged_fold_over_a_contiguous_chain_equals_compute_record() {
        let all = chain(120);
        let full = compute_record(&all, 10, 1);
        assert!(full.windows.len() > 1 && !full.blocks.is_empty());
        for page_size in [7usize, 10, 19, 50, 120, 500] {
            let (rec, cursor, _) = rebuild(&all, page_size, 10);
            assert_same(&rec, &full, &format!("page_size={page_size}"));
            assert_eq!(cursor, Some(119), "page_size={page_size}");
        }
    }

    #[test]
    fn paged_fold_matches_when_more_windows_exist_than_top_k() {
        // w=3 over 1200 blocks: up to 400 disjoint windows, more than TOP_K, so the ranking is
        // truncated and a naive per-page greedy would diverge from the global one.
        let all = chain(1200);
        let full = compute_record(&all, 3, 1);
        assert_eq!(full.windows.len(), TOP_K);
        for page_size in [7usize, 19, 97] {
            let (rec, _, _) = rebuild(&all, page_size, 3);
            assert_same(&rec, &full, &format!("w=3 page_size={page_size}"));
        }
    }

    #[test]
    fn duplicate_block_numbers_at_a_page_boundary_are_not_dropped_or_double_counted() {
        // Explicit: rows 6 and 7 share a block number, page size 7 ends exactly between them.
        let mut all = chain(30);
        let dup = Point { slot: 9000, total_txs: all[6].total_txs + 5, ..all[6] };
        all.insert(7, dup);
        assert_eq!(all[6].block, all[7].block);
        let full = compute_record(&all, 10, 1);
        let (rec, cursor, _) = rebuild(&all, 7, 10);
        assert_same(&rec, &full, "explicit boundary duplicate");
        assert_eq!(rec.hist.iter().sum::<i64>(), all.len() as i64, "every row counted exactly once");
        assert_eq!(cursor, Some(29));

        // Sweep: a duplicate every 4th block, every page size from 5 to 29, so duplicates land on
        // every possible boundary offset.
        let all = chain_with_duplicates(160, 4);
        let full = compute_record(&all, 10, 1);
        for page_size in 5..30usize {
            let (rec, cursor, _) = rebuild(&all, page_size, 10);
            assert_same(&rec, &full, &format!("dup sweep page_size={page_size}"));
            assert_eq!(rec.hist.iter().sum::<i64>(), all.len() as i64, "page_size={page_size}");
            assert_eq!(cursor, Some(159));
        }
    }

    #[test]
    fn rebuild_of_nothing_is_empty_with_no_cursor() {
        let (rec, cursor, pages) = rebuild(&[], 7, 10);
        assert!(rec.windows.is_empty() && rec.blocks.is_empty() && rec.hist == [0; 6]);
        assert_eq!((cursor, pages), (None, 0));
    }

    fn blocks_of(v: &[Point]) -> Vec<i64> {
        v.iter().map(|x| x.block).collect()
    }

    #[test]
    fn full_page_drops_its_last_block_number_so_the_next_page_rereads_it_whole() {
        let mut page: Vec<Point> = [1, 1, 2, 2, 3, 3].iter().map(|&b| p(b, b, b, 1, 1)).collect();
        trim_full_page(&mut page, 6);
        assert_eq!(blocks_of(&page), vec![1, 1, 2, 2]);
        assert_eq!(page.last().unwrap().block, 2, "cursor = largest remaining block number");

        let mut page: Vec<Point> = [1, 2, 3, 4].iter().map(|&b| p(b, b, b, 1, 1)).collect();
        trim_full_page(&mut page, 4);
        assert_eq!(blocks_of(&page), vec![1, 2, 3]);
    }

    #[test]
    fn a_page_that_is_not_full_is_complete_and_left_alone() {
        let mut page: Vec<Point> = [1, 2, 2].iter().map(|&b| p(b, b, b, 1, 1)).collect();
        trim_full_page(&mut page, 4);
        assert_eq!(blocks_of(&page), vec![1, 2, 2]);
        let mut empty: Vec<Point> = vec![];
        trim_full_page(&mut empty, 4);
        assert!(empty.is_empty());
    }

    #[test]
    fn a_full_page_of_a_single_block_number_is_kept_so_the_cursor_still_advances() {
        let mut page: Vec<Point> = (0..4).map(|i| p(9, i, 9, 1, 1)).collect();
        trim_full_page(&mut page, 4);
        assert_eq!(blocks_of(&page), vec![9, 9, 9, 9]);

        // Drop only the last block's rows, not an earlier run of the same size.
        let mut page: Vec<Point> = [5, 5, 5, 6].iter().map(|&b| p(b, b, b, 1, 1)).collect();
        trim_full_page(&mut page, 4);
        assert_eq!(blocks_of(&page), vec![5, 5, 5]);
    }
}
