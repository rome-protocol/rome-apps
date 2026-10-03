//! `resolve(token)` — capture §1.2's event-driven fixed point. See the
//! module doc for exactly what's resolved vs. deferred.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::decode::{decode_log, DecodeError};
use crate::registry::AbiRegistry;
use crate::types::{ArgValue, RawLog, SourceKind};

use super::graph::{ResolvedFrom, ResolvedGraph, ResolvedSource, ScopeFilter, SourceInterval};
use super::registry_source::RegistrySource;
use super::rpc::{LogEntry, ResolverRpc, RpcError};
use super::walk::{fold_intervals, SetEvent};

const ZERO_ADDRESS: [u8; 20] = [0u8; 20];

/// Cycle/runaway guard for [`discover_uv2_pairs`]'s iterative fixed point
/// (review flag, P3b: unlike P3 core's static depth-2 walk, spine-driven
/// discovery can chain — a newly-discovered pair's own LP-share `Transfer`
/// log may surface a further counterparty that resolves to a SECOND pair).
/// The `scanned`/`probed` de-duplication sets already make termination
/// structural (a finite address universe, each address scanned/probed at
/// most once) — this cap is defense-in-depth against a bug in that
/// bookkeeping, not the primary termination mechanism.
const MAX_UV2_DISCOVERY_ROUNDS: usize = 16;

/// `keccak256("TRANSFER_RESTRICTION")` — `RestrictionTypes.TRANSFER_RESTRICTION_TYPE`
/// (Arc contracts `contracts/src/restrictions/RestrictionTypes.sol:12`).
const TRANSFER_RESTRICTION_TYPE: [u8; 32] =
    crate::abi::topic0_from_hex("16d3efd52fe4afa679136c32a17cfe3bac40019518e3dd5b5d42aeb676bcb941");
/// `keccak256("YIELD_RESTRICTION")` — `RestrictionTypes.YIELD_RESTRICTION_TYPE`
/// (Arc contracts `contracts/src/restrictions/RestrictionTypes.sol:13`; this is
/// the yield-BLACKLIST module's type id, capture §2.3).
const YIELD_RESTRICTION_TYPE: [u8; 32] =
    crate::abi::topic0_from_hex("a739d7f0735ea0a32aa51388a348f668f34d9134fa6862aaac41d0b0d91bb0e9");

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("token 0x{token} is not authoritative: not registry-listed and getTokenImplementation(token) == 0 on the registry-resolved factory (capture §1.1)")]
    NotAuthoritative { token: String },
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("event {event} arg `{arg}` missing or wrong-shaped while walking resolution history")]
    MissingArg { event: String, arg: &'static str },
    #[error(
        "UV2 spine discovery did not converge within {0} rounds — possible cycle or runaway growth"
    )]
    DiscoveryExceededRounds(usize),
    #[error("token 0x{token} resolved a ZERO restrictions_router — either a storage-layout drift (the ERC-7201 slot no longer holds a router) or a non-Arc token slipped past §1.1's gate; refusing to resolve a garbage router rather than silently building a graph around 0x0 (capture §1.2)")]
    ZeroRouter { token: String },
}

/// The §1.1 authoritativeness gate, standalone (S4 — also used by
/// `resolve::discovery` to filter discovered candidates BEFORE they ever
/// enter the resolve set; [`resolve`] below calls this too, so there is
/// exactly one implementation of the gate). Registry-listed OR
/// factory-admitted (`getTokenImplementation(factory, token) != 0`) — never
/// on-chain-only, never registry-only. Pure extraction of the pre-S4 inline
/// check; behavior is unchanged.
pub(crate) async fn passes_authoritativeness_gate(
    token: [u8; 20],
    factory: [u8; 20],
    registry: &dyn RegistrySource,
    rpc: &dyn ResolverRpc,
) -> Result<bool, RpcError> {
    let listed = registry.is_listed_asset(token);
    let impl_addr = rpc.get_token_implementation(factory, token).await?;
    Ok(listed || impl_addr != ZERO_ADDRESS)
}

/// Resolves `token`'s full source graph (capture §1.2). `rpc`/`registry` are
/// the injected seams (§"D5"/"M1" in the module doc) — this function never
/// hardcodes an address or opens a connection itself.
pub async fn resolve(
    token: [u8; 20],
    registry: &dyn RegistrySource,
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
) -> Result<ResolvedGraph, ResolveError> {
    let factory = registry.factory();

    // ---- §1.1 authoritativeness gate ----
    if !passes_authoritativeness_gate(token, factory, registry, rpc).await? {
        return Err(ResolveError::NotAuthoritative {
            token: hex::encode(token),
        });
    }

    let mut sources = Vec::new();
    let mut intervals = Vec::new();

    // The token's own registration block anchors the router's (immutable,
    // no setter) `from_block` — walked from the factory's `TokenRegistered`
    // history, never assumed. Falls back to 0 (genesis) if this token was
    // authorized purely via `getTokenImplementation` without a matching
    // `TokenRegistered` log (e.g. a V1-factory path outside this crate's
    // scope) — documented, not invented.
    let registered_block = token_registered_block(rpc, abi, factory, token).await?;
    let anchor = registered_block.unwrap_or(0);

    // ---- seed sources: the token itself, its immutable router, the
    // registry-resolved factory (all three are captured graph MEMBERS, not
    // just resolution inputs — their own events belong in the Record). ----
    push_source(
        &mut sources,
        &mut intervals,
        SourceKind::ArcToken,
        token,
        ResolvedFrom::TokenRead,
        "ArcToken",
        anchor,
        None,
        None,
    );

    // Router-sanity guard (P3b ride-along): a storage read that decodes to
    // the zero address is NEVER a valid negative here — unlike Axis-2's
    // "no module registered" or Morpho's "no deployment", every ArcToken
    // has a real, immutable router by construction (§1.2). A zero read means
    // either the ERC-7201 slot no longer holds a router (layout drift) or a
    // non-Arc token slipped past §1.1's gate — fail LOUD rather than build a
    // graph around a garbage address. (A known-router allowlist cross-check
    // would strengthen this further, but no such set exists in this crate
    // yet — `RegistrySource` carries no router registry, D5 deferred — so
    // this is deliberately just the non-zero check, not a fabricated one.)
    let router = rpc.restrictions_router(token).await?;
    if router == ZERO_ADDRESS {
        return Err(ResolveError::ZeroRouter {
            token: hex::encode(token),
        });
    }
    push_source(
        &mut sources,
        &mut intervals,
        SourceKind::Router,
        router,
        ResolvedFrom::TokenRead,
        "RestrictionsRouter",
        anchor,
        None,
        None,
    );

    // Factory is chain-global (shared across every token it registers,
    // capture §1.2's C1 shared-event case) — registry-resolved, not read
    // from the token, so it spans from genesis.
    push_source(
        &mut sources,
        &mut intervals,
        SourceKind::Factory,
        factory,
        ResolvedFrom::Registry,
        "ArcTokenFactoryV2",
        0,
        None,
        None,
    );

    // ---- Axis-1 + yield-blacklist module swap history (capture §1.2 step 2) ----
    let axis1_current = rpc
        .get_restriction_module(token, TRANSFER_RESTRICTION_TYPE)
        .await?;
    let axis1_history = walk_module_history(rpc, abi, token, TRANSFER_RESTRICTION_TYPE).await?;
    fold_module_sources(
        &mut sources,
        &mut intervals,
        SourceKind::Axis1Module,
        "WhitelistRestrictions",
        axis1_history,
        axis1_current,
        anchor,
    );

    let yield_blacklist_current = rpc
        .get_restriction_module(token, YIELD_RESTRICTION_TYPE)
        .await?;
    let yield_blacklist_history =
        walk_module_history(rpc, abi, token, YIELD_RESTRICTION_TYPE).await?;
    fold_module_sources(
        &mut sources,
        &mut intervals,
        SourceKind::YieldBlacklist,
        "YieldBlacklistRestrictions",
        yield_blacklist_history,
        yield_blacklist_current,
        anchor,
    );

    // ---- yield-token re-pointing history ----
    let yield_token_logs = rpc
        .logs_for(token, crate::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0)
        .await?;
    let yield_token_events = decode_address_set_events(
        abi,
        SourceKind::ArcToken,
        &yield_token_logs,
        "newYieldToken",
    )?;
    fold_walked_sources(
        &mut sources,
        &mut intervals,
        SourceKind::YieldToken,
        "YieldToken (ERC20)",
        yield_token_events,
        // F2 (P3 review): the yield token is typically a chain-wide ERC20
        // (wUSDC) — its capture scope is bounded to distributions FROM this
        // ArcToken, never the token's whole transfer history. Omitting this in a
        // FROZEN manifest input reintroduces the M2 firehose on the yield leg.
        // P4a: now an ENFORCED join predicate (`ScopeFilter::TransferFrom`),
        // not just documentary text — see `resolve::asset_event`'s Layer 1.
        Some(ScopeFilter::TransferFrom { from: token }),
    );

    // ---- storefront: registry-resolved, static (capture §1.2 step 2) ----
    // No scope_filter on the storefront's OWN interval — its own events
    // (PurchaseMade/TokenSaleEnabled/…) are inherently in scope; the filter
    // below belongs on the PURCHASE TOKEN's chain-wide interval, never here.
    let storefront = registry.storefront();
    push_source(
        &mut sources,
        &mut intervals,
        SourceKind::Storefront,
        storefront,
        ResolvedFrom::Registry,
        "ArcTokenPurchase",
        0,
        None,
        None,
    );

    // ---- purchase-token re-pointing history (on the storefront) ----
    let purchase_token_logs = rpc
        .logs_for(
            storefront,
            crate::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
        )
        .await?;
    let purchase_token_events = decode_address_set_events(
        abi,
        SourceKind::Storefront,
        &purchase_token_logs,
        "newPurchaseToken",
    )?;
    fold_walked_sources(
        &mut sources,
        &mut intervals,
        SourceKind::PurchaseToken,
        "PurchaseToken (ERC20)",
        purchase_token_events,
        Some(ScopeFilter::TransferTouches { party: storefront }),
    );

    // ---- Axis-2 (GlobalSanctions) chain-global module discovery (capture
    // §2.4) — an `eth_call` on the ROUTER, so it slots into the same
    // "resolved via a live on-chain read" bucket as `restrictions_router`
    // itself. Zero address = no module registered on this router; downgrade
    // to "not present" rather than an error, same as the Axis-1/yield
    // module reads above. ----
    let sanctions_module = rpc
        .get_global_module_address(router, crate::abi::router::GLOBAL_SANCTIONS_TYPE)
        .await?;
    if sanctions_module != ZERO_ADDRESS {
        push_source(
            &mut sources,
            &mut intervals,
            SourceKind::GlobalSanctions,
            sanctions_module,
            ResolvedFrom::TokenRead,
            "GlobalSanctions",
            anchor,
            None,
            None,
        );
    }

    // ---- UV2 spine-driven pool discovery (capture §2.8) — iterated to a
    // fixed point, not a single pass (see `discover_uv2_pairs`'s doc). ----
    let candidate_pools = registry.candidate_pools();
    let uv2_pairs = discover_uv2_pairs(rpc, abi, token, &candidate_pools).await?;
    for pair in &uv2_pairs {
        push_source(
            &mut sources,
            &mut intervals,
            SourceKind::Uv2Pair,
            pair.address,
            ResolvedFrom::SpineProbe,
            "UniswapV2Pair",
            pair.first_seen_block,
            None,
            // No ScopeFilter: UV2 pairs are NOT ingest-filtered (P4a §"Layer
            // 2" — bounded volume, join-time scope only), so there's no
            // per-market/per-party predicate to enforce here the way
            // yield/purchase-token or Morpho markets need.
            None,
        );
    }

    // ---- Morpho market discovery (capture §2.9) — chain-global singleton;
    // zero address means no Morpho deployment on this chain, so it is
    // never probed (never blindly walked like every other source here). ----
    let morpho = registry.morpho();
    if morpho != ZERO_ADDRESS {
        let markets = discover_morpho_markets(rpc, abi, morpho, token).await?;
        if !markets.is_empty() {
            sources.push(ResolvedSource {
                source_kind: SourceKind::Morpho,
                address: morpho,
                resolved_from: ResolvedFrom::MorphoCreateMarket,
                abi_ref: "Morpho",
            });
            // P4a: one MorphoMarkets scope filter, carrying EVERY matched
            // market id for this asset — a single interval per distinct
            // from_block (matching this crate's existing "one interval per
            // observed event" shape), not per-market, since the join
            // predicate itself is `id = ANY(market_ids)`.
            let market_ids: Vec<[u8; 32]> = markets.iter().map(|(_, id)| *id).collect();
            for (from_block, _) in markets {
                intervals.push(SourceInterval {
                    address: morpho,
                    from_block,
                    to_block: None,
                    scope_filter: Some(ScopeFilter::MorphoMarkets {
                        market_ids: market_ids.clone(),
                    }),
                });
            }
        }
    }

    Ok(ResolvedGraph {
        token,
        sources,
        intervals,
    })
}

/// Resolves the operator-pinned "sanctions floor": a chain-global
/// RestrictionsRouter that carries the GLOBAL_SANCTIONS module even though
/// no seed token's own router routes to it (the coverage gap this closes —
/// see `config::RegistrySection::global_sanctions_router`'s doc). Returns a
/// synthetic [`ResolvedGraph`] whose `token` field is the router address
/// itself (a sentinel — there is no ArcToken here); the caller
/// (`pass::run_resolve_pass`) pushes it into the SAME `graphs` vec every
/// token graph lands in, BEFORE merge/gap-detection, so it flows through the
/// normal manifest/asset_event/merge/gap-detection path unchanged.
///
/// Both intervals anchor at the caller-supplied `from_block` — the operator-
/// configured deploy block of the pinned router (see
/// `config::RegistrySection::global_sanctions_router_from_block`'s doc; the
/// caller passes `registry.global_sanctions_router_from_block().unwrap_or(0)`,
/// so absent config still anchors at genesis, byte-identical to before this
/// knob existed). On a clean audit-first chain, setting `from_block` to (at
/// or after) the router's real deploy block means this anchor sits at-or-
/// ahead of the ingest watermark, so gap-detection opens NO wasteful
/// `[0, watermark]` scan — pure forward capture. On a retrofit chain (real
/// history predates the current watermark), the SAME anchor still opens a
/// real, correctly-bounded `backfill_gap` row starting at `from_block`
/// (never at 0) covering exactly the history that needs a backfill.
pub async fn resolve_pinned_sanctions_floor(
    router: [u8; 20],
    from_block: i64,
    rpc: &dyn ResolverRpc,
    _abi: &AbiRegistry,
) -> Result<ResolvedGraph, ResolveError> {
    let mut sources = Vec::new();
    let mut intervals = Vec::new();

    // Router: chain-global, anchored at the caller-supplied from_block.
    push_source(
        &mut sources,
        &mut intervals,
        SourceKind::Router,
        router,
        ResolvedFrom::Registry,
        "RestrictionsRouter",
        from_block,
        None,
        None,
    );

    // Axis-2 module discovery — the SAME trait read the token-walk's own
    // GlobalSanctions discovery uses. Pure router state read; no token
    // context.
    let module = rpc
        .get_global_module_address(router, crate::abi::router::GLOBAL_SANCTIONS_TYPE)
        .await?;
    if module != ZERO_ADDRESS {
        push_source(
            &mut sources,
            &mut intervals,
            SourceKind::GlobalSanctions,
            module,
            ResolvedFrom::Registry,
            "GlobalSanctions",
            from_block,
            None,
            None,
        );
    }

    Ok(ResolvedGraph {
        token: router,
        sources,
        intervals,
    })
}

/// One UV2 pair discovered via the Transfer spine (capture §2.8) — the
/// address, plus the earliest block at which this crate observed it in
/// scope (never earlier than that — the pair may have existed before, but
/// nothing ties it to this asset before its first observed touch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiscoveredPair {
    address: [u8; 20],
    first_seen_block: i64,
}

/// Discovers every UV2 pair in `token`'s graph, ITERATED TO A FIXED POINT
/// (review flag — P3 core's discovery was a static, depth-2 walk; this one
/// cannot be, because confirming a pair adds a NEW address whose own
/// Transfer log (LP-share movement, §2.8) may itself surface a further
/// counterparty that resolves to a second pair — a genuine second-order
/// discovery, not reachable from `token`'s own Transfer log in one pass).
///
/// Algorithm: maintain a queue of `(address, decode_kind)` targets whose
/// Transfer-shaped log to scan for counterparties, seeded with the token
/// itself; every counterparty found is queued for a `token0()`/`token1()`
/// probe (§2.8's authority — a revert/mismatch means "not a pair", never
/// propagated as a resolution failure); every CONFIRMED pair's own address
/// is pushed back onto the scan queue (its LP-share Transfer log is now
/// in-scope per §1.3's "capture the full event surface"). Registry-listed
/// candidates (`registry_candidates`) are seeded straight into the probe
/// queue — labels, validated the SAME on-chain way, never trusted (`#136`).
/// Terminates because `scanned`/`probed` are monotonic sets over a finite
/// address universe (the fake/real RPC's own finite log set); the round cap
/// is a defensive backstop, not the reason it terminates.
async fn discover_uv2_pairs(
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
    token: [u8; 20],
    registry_candidates: &[[u8; 20]],
) -> Result<Vec<DiscoveredPair>, ResolveError> {
    let mut confirmed: BTreeMap<[u8; 20], i64> = BTreeMap::new();
    let mut probed: BTreeSet<[u8; 20]> = BTreeSet::new();
    let mut scanned: BTreeSet<[u8; 20]> = BTreeSet::new();
    let mut scan_queue: VecDeque<([u8; 20], SourceKind)> =
        VecDeque::from([(token, SourceKind::ArcToken)]);
    // Registry candidates have no observed block — §1.2's documented
    // fallback-to-anchor-0 pattern (same posture as `token_registered_block`
    // falling back to genesis when no matching event is found).
    let mut pending_probe: Vec<([u8; 20], i64)> =
        registry_candidates.iter().map(|&a| (a, 0)).collect();

    let mut round = 0usize;
    loop {
        if scan_queue.is_empty() && pending_probe.is_empty() {
            break;
        }
        round += 1;
        if round > MAX_UV2_DISCOVERY_ROUNDS {
            return Err(ResolveError::DiscoveryExceededRounds(
                MAX_UV2_DISCOVERY_ROUNDS,
            ));
        }

        while let Some((addr, kind)) = scan_queue.pop_front() {
            if !scanned.insert(addr) {
                continue;
            }
            let logs = rpc
                .logs_for(addr, crate::abi::arc_token::TRANSFER_TOPIC0)
                .await?;
            for log in &logs {
                let decoded = decode_log(abi, kind, &to_raw_log(log))?;
                for arg_name in ["from", "to"] {
                    if let Some(ArgValue::Address(s)) = decoded.args.get(arg_name) {
                        let candidate = parse_hex20(s)?;
                        if candidate != ZERO_ADDRESS
                            && candidate != token
                            && !probed.contains(&candidate)
                        {
                            pending_probe.push((candidate, log.block_number));
                        }
                    }
                }
            }
        }

        let batch = std::mem::take(&mut pending_probe);
        for (candidate, first_seen_block) in batch {
            if !probed.insert(candidate) {
                continue;
            }
            // TODO(hardening): `probed` is PER-PASS — every re-resolution
            // re-probes every Transfer counterparty via token0()/token1()
            // eth_call, even the ones already proven not-a-pair, against the
            // rate-limited live RPC. On a token with many transfer
            // counterparties this is the dominant re-resolve RPC cost. A
            // cross-pass NEGATIVE memo (not-a-pair set) would be correct
            // (token0/token1 are immutable, so not-a-pair is permanent) but
            // has nowhere to live: `resolve()` is a free function, so the set
            // would have to sit on `run::AuditWorker` and be threaded through
            // `run_resolve_pass` → `resolve()` → here, changing a signature
            // ~20 tests call. Deferred as invasive per the task's own
            // escape hatch; the per-pass `probed` set already makes a SINGLE
            // pass terminate and de-duplicate.
            let t0 = rpc.token0(candidate).await;
            let t1 = rpc.token1(candidate).await;
            let is_pair = matches!(t0, Ok(a) if a == token) || matches!(t1, Ok(a) if a == token);
            if is_pair {
                confirmed.entry(candidate).or_insert(first_seen_block);
                scan_queue.push_back((candidate, SourceKind::Uv2Pair));
            }
        }
    }

    Ok(confirmed
        .into_iter()
        .map(|(address, first_seen_block)| DiscoveredPair {
            address,
            first_seen_block,
        })
        .collect())
}

/// Discovers Morpho markets where `token` is the loan OR collateral asset
/// (capture §2.9). Returns each matching market's `(CreateMarket block,
/// market_id)` pair — resolve() folds these into one `Morpho` source with
/// multiple intervals (same shape as a re-affirmed yield/purchase token) AND
/// a `ScopeFilter::MorphoMarkets` carrying every matched id (P4a: the id is
/// now load-bearing for the caller, not just the block). The decision uses
/// `idToMarketParams` — the on-chain read — never the event's own
/// (redundant) `marketParams` field (`#136`'s "read is authority" lesson).
async fn discover_morpho_markets(
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
    morpho: [u8; 20],
    token: [u8; 20],
) -> Result<Vec<(i64, [u8; 32])>, ResolveError> {
    let logs = rpc
        .logs_for(morpho, crate::abi::morpho::CREATE_MARKET_TOPIC0)
        .await?;

    let mut matching = Vec::new();
    for log in &logs {
        let decoded = decode_log(abi, SourceKind::Morpho, &to_raw_log(log))?;
        let market_id = match decoded.args.get("id") {
            Some(ArgValue::Bytes32(s)) => parse_hex32(s)?,
            _ => {
                return Err(ResolveError::MissingArg {
                    event: decoded.event_name.clone(),
                    arg: "id",
                })
            }
        };
        let params = rpc.id_to_market_params(morpho, market_id).await?;
        if params.loan_token == token || params.collateral_token == token {
            matching.push((log.block_number, market_id));
        }
    }
    Ok(matching)
}

/// Maps a [`ResolvedGraph`] to the `(lowercase-0x-address -> SourceKind)`
/// shape `ingest::pipeline::IngestConfig::sources` expects — the wiring that
/// makes `capture(token)` end-to-end (ingest consumes the RESOLVED graph
/// instead of a caller-supplied map).
pub fn to_ingest_sources(graph: &ResolvedGraph) -> BTreeMap<String, SourceKind> {
    graph
        .sources
        .iter()
        .map(|s| (format!("0x{}", hex::encode(s.address)), s.source_kind))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn push_source(
    sources: &mut Vec<ResolvedSource>,
    intervals: &mut Vec<SourceInterval>,
    source_kind: SourceKind,
    address: [u8; 20],
    resolved_from: ResolvedFrom,
    abi_ref: &'static str,
    from_block: i64,
    to_block: Option<i64>,
    scope_filter: Option<ScopeFilter>,
) {
    sources.push(ResolvedSource {
        source_kind,
        address,
        resolved_from,
        abi_ref,
    });
    intervals.push(SourceInterval {
        address,
        from_block,
        to_block,
        scope_filter,
    });
}

/// Folds a module's swap history into sources+intervals. If the walk found
/// no `SpecificRestrictionModuleSet` events for this type id but the LIVE
/// read is non-zero, falls back to a single open interval from `anchor` —
/// the defensive path for a module set outside the event stream this crate
/// walks (documented in the module doc as a fallback, never silently
/// dropped).
#[allow(clippy::too_many_arguments)]
fn fold_module_sources(
    sources: &mut Vec<ResolvedSource>,
    intervals: &mut Vec<SourceInterval>,
    kind: SourceKind,
    abi_ref: &'static str,
    history: Vec<([u8; 20], i64, Option<i64>)>,
    current_live: [u8; 20],
    anchor: i64,
) {
    if history.is_empty() {
        if current_live != ZERO_ADDRESS {
            fold_walked_sources(
                sources,
                intervals,
                kind,
                abi_ref,
                vec![SetEvent {
                    block_number: anchor,
                    tx_index: 0,
                    log_index: 0,
                    address: current_live,
                }],
                None,
            );
        }
        return;
    }
    let mut seen = std::collections::BTreeSet::new();
    for (address, from_block, to_block) in history {
        if seen.insert(address) {
            sources.push(ResolvedSource {
                source_kind: kind,
                address,
                resolved_from: ResolvedFrom::TokenEvent,
                abi_ref,
            });
        }
        intervals.push(SourceInterval {
            address,
            from_block,
            to_block,
            scope_filter: None,
        });
    }
}

/// Folds a plain set-event history (yield-token / purchase-token) into
/// sources+intervals — one [`ResolvedSource`] per distinct address, one
/// [`SourceInterval`] per event (multiple intervals may share an address if
/// it was re-affirmed later).
fn fold_walked_sources(
    sources: &mut Vec<ResolvedSource>,
    intervals: &mut Vec<SourceInterval>,
    kind: SourceKind,
    abi_ref: &'static str,
    events: Vec<SetEvent>,
    scope_filter: Option<ScopeFilter>,
) {
    let triples = fold_intervals(events);
    let mut seen = std::collections::BTreeSet::new();
    for (address, from_block, to_block) in triples {
        if seen.insert(address) {
            sources.push(ResolvedSource {
                source_kind: kind,
                address,
                resolved_from: ResolvedFrom::TokenEvent,
                abi_ref,
            });
        }
        intervals.push(SourceInterval {
            address,
            from_block,
            to_block,
            scope_filter: scope_filter.clone(),
        });
    }
}

/// Walks `token`'s `SpecificRestrictionModuleSet` history, filters to
/// `type_id`, and folds the matches into intervals (capture §1.2: "seed via
/// `getRestrictionModule`, then walk `SpecificRestrictionModuleSet` for
/// every past module + its interval" — this is the walk half; the seed is
/// the caller's `current_live` fallback in [`fold_module_sources`]).
async fn walk_module_history(
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
    token: [u8; 20],
    type_id: [u8; 32],
) -> Result<Vec<([u8; 20], i64, Option<i64>)>, ResolveError> {
    let logs = rpc
        .logs_for(
            token,
            crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
        )
        .await?;

    let mut matching = Vec::new();
    for log in &logs {
        let decoded = decode_log(abi, SourceKind::ArcToken, &to_raw_log(log))?;
        let decoded_type_id = match decoded.args.get("typeId") {
            Some(ArgValue::Bytes32(s)) => parse_hex32(s)?,
            _ => {
                return Err(ResolveError::MissingArg {
                    event: decoded.event_name.clone(),
                    arg: "typeId",
                })
            }
        };
        if decoded_type_id != type_id {
            continue;
        }
        let module = match decoded.args.get("moduleAddress") {
            Some(ArgValue::Address(s)) => parse_hex20(s)?,
            _ => {
                return Err(ResolveError::MissingArg {
                    event: decoded.event_name.clone(),
                    arg: "moduleAddress",
                })
            }
        };
        matching.push(SetEvent {
            block_number: log.block_number,
            tx_index: log.tx_index,
            log_index: log.log_index,
            address: module,
        });
    }
    Ok(fold_intervals(matching))
}

/// Decodes a single-`Address`-arg event ([`YieldTokenUpdated`]/
/// [`PurchaseTokenUpdated`]-shaped) into [`SetEvent`]s.
fn decode_address_set_events(
    abi: &AbiRegistry,
    source_kind: SourceKind,
    logs: &[LogEntry],
    arg_name: &'static str,
) -> Result<Vec<SetEvent>, ResolveError> {
    let mut out = Vec::with_capacity(logs.len());
    for log in logs {
        let decoded = decode_log(abi, source_kind, &to_raw_log(log))?;
        let address = match decoded.args.get(arg_name) {
            Some(ArgValue::Address(s)) => parse_hex20(s)?,
            _ => {
                return Err(ResolveError::MissingArg {
                    event: decoded.event_name.clone(),
                    arg: arg_name,
                })
            }
        };
        out.push(SetEvent {
            block_number: log.block_number,
            tx_index: log.tx_index,
            log_index: log.log_index,
            address,
        });
    }
    Ok(out)
}

/// Walks the factory's `TokenRegistered` history looking for `token`'s own
/// registration log; returns its block number (the router's immutable
/// `from_block` anchor). `None` if not found (fallback to genesis in the
/// caller — documented, not invented).
async fn token_registered_block(
    rpc: &dyn ResolverRpc,
    abi: &AbiRegistry,
    factory: [u8; 20],
    token: [u8; 20],
) -> Result<Option<i64>, ResolveError> {
    let logs = rpc
        .logs_for(factory, crate::abi::factory::TOKEN_REGISTERED_TOPIC0)
        .await?;
    for log in &logs {
        let decoded = decode_log(abi, SourceKind::Factory, &to_raw_log(log))?;
        if let Some(ArgValue::Address(s)) = decoded.args.get("token") {
            if parse_hex20(s)? == token {
                return Ok(Some(log.block_number));
            }
        }
    }
    Ok(None)
}

fn to_raw_log(log: &LogEntry) -> RawLog {
    RawLog {
        address: log.address,
        topics: log.topics.clone(),
        data: log.data.clone(),
    }
}

fn parse_hex20(s: &str) -> Result<[u8; 20], ResolveError> {
    let bytes = hex::decode(s.trim_start_matches("0x")).map_err(|_| ResolveError::MissingArg {
        event: "<hex decode>".to_string(),
        arg: "address",
    })?;
    bytes.try_into().map_err(|_| ResolveError::MissingArg {
        event: "<hex decode>".to_string(),
        arg: "address",
    })
}

fn parse_hex32(s: &str) -> Result<[u8; 32], ResolveError> {
    let bytes = hex::decode(s.trim_start_matches("0x")).map_err(|_| ResolveError::MissingArg {
        event: "<hex decode>".to_string(),
        arg: "bytes32",
    })?;
    bytes.try_into().map_err(|_| ResolveError::MissingArg {
        event: "<hex decode>".to_string(),
        arg: "bytes32",
    })
}

#[cfg(test)]
mod tests {
    //! Pure resolution tests against [`super::super::fixture`]'s fakes —
    //! zero DB, zero network. The DB-facing `capture_manifest`/`asset_event`
    //! persistence tests live in `tests/resolve_db.rs` (real Postgres, per
    //! the task).

    use crate::abi::build_registry;
    use crate::resolve::fixture::{FakeRegistry, FakeRpc};
    use crate::resolve::rpc::LogEntry;
    use crate::types::SourceKind;

    use super::*;

    fn addr(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn address_word(a: [u8; 20]) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[12..].copy_from_slice(&a);
        w
    }

    /// A minimal, fully-wired fixture: one token, one factory, one router,
    /// registered via a `TokenRegistered` log at `registered_block`. Callers
    /// add module-swap / yield-token / purchase-token history logs on top.
    struct Fixture {
        rpc: FakeRpc,
        registry: FakeRegistry,
        abi: crate::registry::AbiRegistry,
        token: [u8; 20],
        factory: [u8; 20],
        router: [u8; 20],
        storefront: [u8; 20],
    }

    fn base_fixture(registered_block: i64) -> Fixture {
        let token = addr(0xA0);
        let factory = addr(0xF0);
        let router = addr(0xB0);
        let storefront = addr(0xC0);
        let impl_addr = addr(0xD0);

        let mut rpc = FakeRpc::new();
        rpc.set_token_implementation(factory, token, impl_addr);
        rpc.set_router(token, router);
        rpc.add_log(
            factory,
            crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
            LogEntry {
                block_number: registered_block,
                tx_index: 0,
                log_index: 0,
                address: factory,
                topics: vec![
                    crate::abi::factory::TOKEN_REGISTERED_TOPIC0,
                    address_word(token),
                    address_word(impl_addr),
                ],
                data: vec![],
            },
        );

        let registry = FakeRegistry::new("deadbeef", factory, storefront);

        Fixture {
            rpc,
            registry,
            abi: build_registry(),
            token,
            factory,
            router,
            storefront,
        }
    }

    fn module_set_log(
        block_number: i64,
        log_index: i32,
        type_id: [u8; 32],
        module: [u8; 20],
    ) -> LogEntry {
        LogEntry {
            block_number,
            tx_index: 0,
            log_index,
            address: [0u8; 20], // filled by caller's `add_log(token, ...)`
            topics: vec![
                crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
                type_id,
                address_word(module),
            ],
            data: vec![],
        }
    }

    /// An ERC-20 `Transfer(from, to, value)` log (P3b UV2/spine tests) — the
    /// `address` field is filled by the caller's `add_log(source_addr, ...)`
    /// struct-update, same convention as `module_set_log`.
    fn transfer_log(block_number: i64, log_index: i32, from: [u8; 20], to: [u8; 20]) -> LogEntry {
        LogEntry {
            block_number,
            tx_index: 0,
            log_index,
            address: [0u8; 20],
            topics: vec![
                crate::abi::arc_token::TRANSFER_TOPIC0,
                address_word(from),
                address_word(to),
            ],
            data: vec![0u8; 32], // value = 0, irrelevant to discovery
        }
    }

    /// A Morpho `CreateMarket(id, marketParams)` log (P3b Morpho discovery
    /// tests). `marketParams`' 5 static words are irrelevant to discovery
    /// (the decision reads `idToMarketParams` live, `#136`), so they're
    /// zeroed here.
    fn create_market_log(block_number: i64, log_index: i32, market_id: [u8; 32]) -> LogEntry {
        LogEntry {
            block_number,
            tx_index: 0,
            log_index,
            address: [0u8; 20],
            topics: vec![crate::abi::morpho::CREATE_MARKET_TOPIC0, market_id],
            data: vec![0u8; 32 * 5],
        }
    }

    // ---- Authoritativeness gate (§1.1) ----

    #[tokio::test]
    async fn unregistered_and_factory_unknown_token_is_rejected() {
        let fx = base_fixture(100);
        // A token neither registry-listed nor known to `getTokenImplementation`.
        let unknown_token = addr(0xEE);

        let err = resolve(unknown_token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap_err();
        assert!(matches!(err, ResolveError::NotAuthoritative { .. }));
    }

    #[tokio::test]
    async fn registry_listed_token_passes_the_gate_even_with_zero_implementation() {
        let mut fx = base_fixture(100);
        let listed_only_token = addr(0xEF);
        fx.registry.list_asset(listed_only_token);
        fx.rpc.set_router(listed_only_token, fx.router);
        // Deliberately NOT set in `token_implementations` — resolves to the
        // zero address; registry listing alone must still pass the gate.

        let graph = resolve(listed_only_token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();
        assert_eq!(graph.token, listed_only_token);
    }

    #[tokio::test]
    async fn factory_known_but_unlisted_token_passes_the_gate() {
        let fx = base_fixture(100);
        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();
        assert_eq!(graph.token, fx.token);
    }

    // ---- Fixed-point: Axis-1 module swap mid-history ----

    #[tokio::test]
    async fn module_swap_mid_history_yields_both_modules_with_correct_intervals() {
        let mut fx = base_fixture(100);
        let module_a = addr(0x11);
        let module_b = addr(0x22);

        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
            LogEntry {
                address: fx.token,
                ..module_set_log(150, 0, TRANSFER_RESTRICTION_TYPE, module_a)
            },
        );
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
            LogEntry {
                address: fx.token,
                ..module_set_log(300, 0, TRANSFER_RESTRICTION_TYPE, module_b)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        let axis1_intervals: Vec<_> = graph
            .intervals
            .iter()
            .filter(|i| i.address == module_a || i.address == module_b)
            .collect();
        assert_eq!(axis1_intervals.len(), 2);
        assert!(axis1_intervals
            .iter()
            .any(|i| i.address == module_a && i.from_block == 150 && i.to_block == Some(300)));
        assert!(axis1_intervals
            .iter()
            .any(|i| i.address == module_b && i.from_block == 300 && i.to_block.is_none()));

        let axis1_sources: Vec<_> = graph
            .sources
            .iter()
            .filter(|s| s.source_kind == SourceKind::Axis1Module)
            .collect();
        assert_eq!(axis1_sources.len(), 2);
    }

    #[tokio::test]
    async fn yield_blacklist_module_swap_is_independent_of_axis1() {
        let mut fx = base_fixture(100);
        let axis1_module = addr(0x11);
        let yb_module = addr(0x33);

        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
            LogEntry {
                address: fx.token,
                ..module_set_log(150, 0, TRANSFER_RESTRICTION_TYPE, axis1_module)
            },
        );
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
            LogEntry {
                address: fx.token,
                ..module_set_log(160, 1, YIELD_RESTRICTION_TYPE, yb_module)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == axis1_module && s.source_kind == SourceKind::Axis1Module));
        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == yb_module && s.source_kind == SourceKind::YieldBlacklist));
        // Neither module is mislabeled as the other's kind.
        assert!(!graph
            .sources
            .iter()
            .any(|s| s.address == axis1_module && s.source_kind == SourceKind::YieldBlacklist));
    }

    // ---- Fixed-point: yield-token changed once ----

    #[tokio::test]
    async fn yield_token_changed_once_yields_two_yield_token_sources_with_intervals() {
        let mut fx = base_fixture(100);
        let yield_token_1 = addr(0x44);
        let yield_token_2 = addr(0x55);

        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
            LogEntry {
                block_number: 120,
                tx_index: 0,
                log_index: 0,
                address: fx.token,
                topics: vec![
                    crate::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
                    address_word(yield_token_1),
                ],
                data: vec![],
            },
        );
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
            LogEntry {
                block_number: 400,
                tx_index: 0,
                log_index: 0,
                address: fx.token,
                topics: vec![
                    crate::abi::arc_token::YIELD_TOKEN_UPDATED_TOPIC0,
                    address_word(yield_token_2),
                ],
                data: vec![],
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        let yt_sources: Vec<_> = graph
            .sources
            .iter()
            .filter(|s| s.source_kind == SourceKind::YieldToken)
            .collect();
        assert_eq!(yt_sources.len(), 2);

        let yt_intervals: Vec<_> = graph
            .intervals
            .iter()
            .filter(|i| i.address == yield_token_1 || i.address == yield_token_2)
            .collect();
        assert!(yt_intervals
            .iter()
            .any(|i| i.address == yield_token_1 && i.from_block == 120 && i.to_block == Some(400)));
        assert!(yt_intervals
            .iter()
            .any(|i| i.address == yield_token_2 && i.from_block == 400 && i.to_block.is_none()));
    }

    // ---- Purchase-token history + shared chain-global sources ----

    #[tokio::test]
    async fn purchase_token_changed_once_yields_two_purchase_token_sources_with_intervals() {
        let mut fx = base_fixture(100);
        let purchase_token_1 = addr(0x66);
        let purchase_token_2 = addr(0x77);

        fx.rpc.add_log(
            fx.storefront,
            crate::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
            LogEntry {
                block_number: 130,
                tx_index: 0,
                log_index: 0,
                address: fx.storefront,
                topics: vec![
                    crate::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
                    address_word(purchase_token_1),
                ],
                data: vec![],
            },
        );
        fx.rpc.add_log(
            fx.storefront,
            crate::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
            LogEntry {
                block_number: 500,
                tx_index: 0,
                log_index: 0,
                address: fx.storefront,
                topics: vec![
                    crate::abi::storefront::PURCHASE_TOKEN_UPDATED_TOPIC0,
                    address_word(purchase_token_2),
                ],
                data: vec![],
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        let pt_intervals: Vec<_> = graph
            .intervals
            .iter()
            .filter(|i| i.address == purchase_token_1 || i.address == purchase_token_2)
            .collect();
        assert!(pt_intervals.iter().any(|i| i.address == purchase_token_1
            && i.from_block == 130
            && i.to_block == Some(500)));
        assert!(pt_intervals
            .iter()
            .any(|i| i.address == purchase_token_2 && i.from_block == 500 && i.to_block.is_none()));

        // Storefront itself is a resolved (registry) source alongside the
        // purchase tokens it re-pointed to.
        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == fx.storefront && s.source_kind == SourceKind::Storefront));
    }

    #[tokio::test]
    async fn factory_and_router_are_resolved_as_chain_global_sources() {
        let fx = base_fixture(100);
        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == fx.factory && s.source_kind == SourceKind::Factory));
        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == fx.router && s.source_kind == SourceKind::Router));
        // The router's interval is anchored at the token's OWN registration
        // block (from the factory's `TokenRegistered` walk), not genesis.
        assert!(graph
            .intervals
            .iter()
            .any(|i| i.address == fx.router && i.from_block == 100 && i.to_block.is_none()));
    }

    // ---- No-hardcode canary ----

    #[tokio::test]
    async fn no_hardcode_canary_every_resolved_address_traces_to_a_configured_input() {
        let mut fx = base_fixture(777); // an arbitrary block no source code literal could match
        let module = addr(0x91);
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
            LogEntry {
                address: fx.token,
                ..module_set_log(800, 0, TRANSFER_RESTRICTION_TYPE, module)
            },
        );

        // P3b: extend the canary to the new source kinds too — a hardcode in
        // Axis-2/UV2/Morpho discovery must be just as loud as one in the P3
        // core paths above.
        let sanctions_module = addr(0x92);
        fx.rpc.set_global_module(
            fx.router,
            crate::abi::router::GLOBAL_SANCTIONS_TYPE,
            sanctions_module,
        );

        let pair = addr(0x93);
        fx.rpc.set_pair(pair, fx.token, addr(0xF9));
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: fx.token,
                ..transfer_log(810, 0, addr(0x94), pair)
            },
        );

        let morpho = addr(0x95);
        fx.registry.set_morpho(morpho);
        let market_id = [0x96u8; 32];
        fx.rpc.set_market(market_id, fx.token, addr(0xF8));
        fx.rpc.add_log(
            morpho,
            crate::abi::morpho::CREATE_MARKET_TOPIC0,
            LogEntry {
                address: morpho,
                ..create_market_log(820, 0, market_id)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();
        assert!(graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::GlobalSanctions));
        assert!(graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::Uv2Pair));
        assert!(graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::Morpho));

        let mut known = fx.rpc.known_addresses();
        known.insert(fx.registry.factory());
        known.insert(fx.registry.storefront());
        known.insert(fx.registry.morpho());

        for source in &graph.sources {
            assert!(
                known.contains(&source.address),
                "resolved address {:?} (kind {:?}) wasn't supplied by the fake — possible hardcode",
                source.address,
                source.source_kind
            );
        }
        for interval in &graph.intervals {
            assert!(
                known.contains(&interval.address),
                "resolved interval address {:?} wasn't supplied by the fake — possible hardcode",
                interval.address
            );
        }
    }

    // ---- Router-sanity guard ----

    #[tokio::test]
    async fn zero_router_from_storage_fails_loud_instead_of_resolving_garbage() {
        let mut fx = base_fixture(100);
        fx.rpc.set_router(fx.token, ZERO_ADDRESS); // simulates a layout-drift/garbage storage read

        let err = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap_err();

        assert!(matches!(err, ResolveError::ZeroRouter { .. }));
    }

    // ---- Axis-2 (GlobalSanctions) chain-global module discovery (§2.4) ----

    #[tokio::test]
    async fn router_with_a_registered_sanctions_module_gets_it_as_a_source() {
        let mut fx = base_fixture(100);
        let module = addr(0x60);
        fx.rpc
            .set_global_module(fx.router, crate::abi::router::GLOBAL_SANCTIONS_TYPE, module);

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == module && s.source_kind == SourceKind::GlobalSanctions));
    }

    #[tokio::test]
    async fn router_with_no_sanctions_module_adds_nothing() {
        let fx = base_fixture(100); // no set_global_module call at all
        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(!graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::GlobalSanctions));
    }

    // ---- 6a: pinned sanctions-router coverage-gap closer
    // (`resolve_pinned_sanctions_floor`) ----

    /// Renamed from `..._both_from_block_zero` (S6a): the fn no longer
    /// hardcodes `from_block = 0` — it passes the caller-supplied
    /// `from_block` straight through to BOTH the Router and the discovered
    /// GlobalSanctions module's interval. `from_block = 0` is exercised here
    /// as one case of pass-through, not as the fn's own decision.
    #[tokio::test]
    async fn pinned_sanctions_floor_passes_from_block_through_to_both_router_and_module_intervals(
    ) {
        for from_block in [0i64, 485_480_000i64] {
            let pinned_router = addr(0x70);
            let module = addr(0x71);
            let mut rpc = FakeRpc::new();
            rpc.set_global_module(
                pinned_router,
                crate::abi::router::GLOBAL_SANCTIONS_TYPE,
                module,
            );
            let abi = build_registry();

            let graph = resolve_pinned_sanctions_floor(pinned_router, from_block, &rpc, &abi)
                .await
                .unwrap();

            assert_eq!(graph.token, pinned_router);
            assert!(graph
                .sources
                .iter()
                .any(|s| s.address == pinned_router && s.source_kind == SourceKind::Router));
            assert!(graph
                .sources
                .iter()
                .any(|s| s.address == module && s.source_kind == SourceKind::GlobalSanctions));
            assert!(
                graph.intervals.iter().any(|i| i.address == pinned_router
                    && i.from_block == from_block
                    && i.to_block.is_none()),
                "the router's own interval must carry the caller-supplied from_block \
                 ({from_block}), got {:?}",
                graph.intervals
            );
            assert!(
                graph
                    .intervals
                    .iter()
                    .any(|i| i.address == module && i.from_block == from_block && i.to_block.is_none()),
                "the discovered module's interval must ALSO carry the caller-supplied \
                 from_block ({from_block}) — this is what anchors the backfill gap at the \
                 deploy block instead of always genesis"
            );
        }
    }

    #[tokio::test]
    async fn pinned_sanctions_floor_with_no_module_registered_yields_router_only() {
        let pinned_router = addr(0x72);
        let rpc = FakeRpc::new(); // no set_global_module call at all
        let abi = build_registry();

        let graph = resolve_pinned_sanctions_floor(pinned_router, 0, &rpc, &abi)
            .await
            .unwrap();

        assert_eq!(graph.sources.len(), 1, "only the router itself, no GlobalSanctions source");
        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == pinned_router && s.source_kind == SourceKind::Router));
        assert!(!graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::GlobalSanctions));
    }

    // ---- UV2 spine-driven pool discovery (§2.8) ----

    #[tokio::test]
    async fn transfer_counterparty_whose_token0_matches_the_asset_is_a_discovered_pair() {
        let mut fx = base_fixture(100);
        let pair = addr(0x61);
        fx.rpc.set_pair(pair, fx.token, addr(0xF0));
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: fx.token,
                ..transfer_log(150, 0, addr(0xAA), pair)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == pair && s.source_kind == SourceKind::Uv2Pair));
        assert!(graph
            .intervals
            .iter()
            .any(|i| i.address == pair && i.from_block == 150 && i.to_block.is_none()));
    }

    #[tokio::test]
    async fn transfer_counterparty_that_reverts_on_token0_and_token1_is_not_a_pair() {
        let mut fx = base_fixture(100);
        let not_a_pair = addr(0x62); // never given to `set_pair` — token0()/token1() revert
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: fx.token,
                ..transfer_log(150, 0, addr(0xAA), not_a_pair)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(!graph.sources.iter().any(|s| s.address == not_a_pair));
    }

    #[tokio::test]
    async fn transfer_counterparty_whose_tokens_dont_match_the_asset_is_not_a_pair() {
        let mut fx = base_fixture(100);
        let unrelated_pair = addr(0x63);
        // A real, callable UV2 pair — just not one that trades THIS asset.
        fx.rpc.set_pair(unrelated_pair, addr(0xE1), addr(0xE2));
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: fx.token,
                ..transfer_log(150, 0, addr(0xAA), unrelated_pair)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(!graph.sources.iter().any(|s| s.address == unrelated_pair));
    }

    /// `#136`: a registry-LISTED pool whose on-chain `token0`/`token1` don't
    /// include the asset is REJECTED — the on-chain read is authority, the
    /// registry listing is only a label.
    #[tokio::test]
    async fn registry_listed_pool_that_fails_the_onchain_check_is_rejected() {
        let mut fx = base_fixture(100);
        let listed_but_wrong = addr(0x64);
        fx.rpc.set_pair(listed_but_wrong, addr(0xE1), addr(0xE2)); // neither side is `fx.token`
        fx.registry.add_candidate_pool(listed_but_wrong);

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(!graph.sources.iter().any(|s| s.address == listed_but_wrong));
    }

    /// The registry-candidate path also works POSITIVELY — a registry
    /// listing that passes on-chain validation is discovered even with NO
    /// Transfer-log evidence at all (the label got it there; the on-chain
    /// read confirmed it).
    #[tokio::test]
    async fn registry_listed_pool_that_passes_the_onchain_check_is_discovered() {
        let mut fx = base_fixture(100);
        let listed_and_valid = addr(0x65);
        fx.rpc.set_pair(listed_and_valid, addr(0xE1), fx.token);
        fx.registry.add_candidate_pool(listed_and_valid);

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == listed_and_valid && s.source_kind == SourceKind::Uv2Pair));
    }

    /// Review flag: a SECOND pair, reachable ONLY by walking the FIRST
    /// discovered pair's own LP-share `Transfer` log — never a direct
    /// counterparty of `token`'s own Transfer log. Proves the fixed point
    /// actually iterates, not just a single pass.
    #[tokio::test]
    async fn second_order_pair_reachable_only_through_the_first_pairs_own_transfer_log() {
        let mut fx = base_fixture(100);
        let pair1 = addr(0x66);
        let lp_holder = addr(0x68);

        // pair1 is a first-order discovery: a Transfer counterparty of `token`.
        fx.rpc.set_pair(pair1, fx.token, addr(0xF0));
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: fx.token,
                ..transfer_log(150, 0, addr(0xAA), pair1)
            },
        );

        // pair1's OWN Transfer log (LP-share movement) has a leg to
        // `lp_holder` — NOT visible from `token`'s own Transfer log at all.
        fx.rpc.add_log(
            pair1,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: pair1,
                ..transfer_log(160, 0, pair1, lp_holder)
            },
        );

        // `lp_holder` is ITSELF a genuine UV2 pair for `token` — reachable
        // only via pair1's Transfer log.
        fx.rpc.set_pair(lp_holder, addr(0xF1), fx.token);

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == pair1 && s.source_kind == SourceKind::Uv2Pair));
        assert!(
            graph
                .sources
                .iter()
                .any(|s| s.address == lp_holder && s.source_kind == SourceKind::Uv2Pair),
            "second-order pair discovered only via pair1's own Transfer log must be reached"
        );
    }

    /// A cycle (pair1's Transfer log points at pair2, pair2's points back at
    /// pair1) must not hang the resolver — `scanned`/`probed` de-duplication
    /// makes each address get walked/probed exactly once regardless.
    #[tokio::test]
    async fn a_cycle_between_two_discovered_pairs_terminates() {
        let mut fx = base_fixture(100);
        let pair1 = addr(0x69);
        let pair2 = addr(0x6A);

        fx.rpc.set_pair(pair1, fx.token, addr(0xF0));
        fx.rpc.add_log(
            fx.token,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: fx.token,
                ..transfer_log(150, 0, addr(0xAA), pair1)
            },
        );
        fx.rpc.set_pair(pair2, addr(0xF1), fx.token);
        // pair1 -> pair2 and pair2 -> pair1: a mutual reference cycle.
        fx.rpc.add_log(
            pair1,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: pair1,
                ..transfer_log(160, 0, pair1, pair2)
            },
        );
        fx.rpc.add_log(
            pair2,
            crate::abi::arc_token::TRANSFER_TOPIC0,
            LogEntry {
                address: pair2,
                ..transfer_log(170, 0, pair2, pair1)
            },
        );

        // Must complete (not hang) and find exactly the two pairs.
        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        let uv2_addresses: std::collections::BTreeSet<_> = graph
            .sources
            .iter()
            .filter(|s| s.source_kind == SourceKind::Uv2Pair)
            .map(|s| s.address)
            .collect();
        assert_eq!(
            uv2_addresses,
            std::collections::BTreeSet::from([pair1, pair2])
        );
    }

    // ---- Morpho market discovery (§2.9) ----

    #[tokio::test]
    async fn morpho_market_with_asset_as_collateral_is_discovered() {
        let mut fx = base_fixture(100);
        let morpho = addr(0x70);
        fx.registry.set_morpho(morpho);
        let market_id = [0x01u8; 32];
        fx.rpc.set_market(market_id, addr(0xE1), fx.token); // collateral = token
        fx.rpc.add_log(
            morpho,
            crate::abi::morpho::CREATE_MARKET_TOPIC0,
            LogEntry {
                address: morpho,
                ..create_market_log(200, 0, market_id)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == morpho && s.source_kind == SourceKind::Morpho));
        assert!(graph
            .intervals
            .iter()
            .any(|i| i.address == morpho && i.from_block == 200));
    }

    #[tokio::test]
    async fn morpho_market_with_asset_as_loan_token_is_discovered() {
        let mut fx = base_fixture(100);
        let morpho = addr(0x71);
        fx.registry.set_morpho(morpho);
        let market_id = [0x02u8; 32];
        fx.rpc.set_market(market_id, fx.token, addr(0xE2)); // loan = token
        fx.rpc.add_log(
            morpho,
            crate::abi::morpho::CREATE_MARKET_TOPIC0,
            LogEntry {
                address: morpho,
                ..create_market_log(210, 0, market_id)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(graph
            .sources
            .iter()
            .any(|s| s.address == morpho && s.source_kind == SourceKind::Morpho));
    }

    #[tokio::test]
    async fn morpho_market_with_asset_as_neither_loan_nor_collateral_is_excluded() {
        let mut fx = base_fixture(100);
        let morpho = addr(0x72);
        fx.registry.set_morpho(morpho);
        let market_id = [0x03u8; 32];
        fx.rpc.set_market(market_id, addr(0xE1), addr(0xE2)); // neither side is `fx.token`
        fx.rpc.add_log(
            morpho,
            crate::abi::morpho::CREATE_MARKET_TOPIC0,
            LogEntry {
                address: morpho,
                ..create_market_log(220, 0, market_id)
            },
        );

        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        assert!(!graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::Morpho));
    }

    #[tokio::test]
    async fn no_morpho_deployment_on_chain_means_no_probe_at_all() {
        // registry.morpho() left at the zero-address default — resolve()
        // must not even call `logs_for`/`id_to_market_params`.
        let fx = base_fixture(100);
        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();
        assert!(!graph
            .sources
            .iter()
            .any(|s| s.source_kind == SourceKind::Morpho));
    }

    // ---- to_ingest_sources wiring ----

    #[tokio::test]
    async fn to_ingest_sources_maps_every_resolved_address_to_its_source_kind() {
        let fx = base_fixture(100);
        let graph = resolve(fx.token, &fx.registry, &fx.rpc, &fx.abi)
            .await
            .unwrap();

        let sources = to_ingest_sources(&graph);
        let token_key = format!("0x{}", hex::encode(fx.token));
        assert_eq!(sources.get(&token_key), Some(&SourceKind::ArcToken));
        let router_key = format!("0x{}", hex::encode(fx.router));
        assert_eq!(sources.get(&router_key), Some(&SourceKind::Router));
    }
}
