-- Invalidate every signature previously resolved from 4byte.directory so the
-- decoder re-resolves them under the new "lowest-id wins" ordering.
--
-- 4byte's default API ordering let later-submitted collision-bait outrank the
-- legit entry (e.g. selector 0x18cbafe5 was returning
-- `join_tg_invmru_haha_617eab6(...)` instead of `swapExactTokensForETH(...)`).
-- The decoder now sorts by 4byte `id` ascending, so legit entries registered
-- in 2017–2020 always beat 2022+ collision-bait. We don't try to enumerate
-- spam patterns — that's the maintenance burden we're avoiding.
--
-- Wiping `source = '4byte'` triggers re-fetch on the next polling cycle.
-- Static seeds and operator-applied 'manual' rows are preserved.
DELETE FROM rome_via.method_signatures
WHERE source = '4byte';
