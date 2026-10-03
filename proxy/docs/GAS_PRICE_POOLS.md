# Gas-Price Pools (Meteora)

Rome derives the gas price **off-chain in the proxy**: a `PriceManager` polls a
Meteora pool every ~15s, derives the gas-token price, and serves it via
`eth_gasPrice` (scaled by `gas_price_mul`). On-chain the `GASPRICE` opcode just
returns the transaction's RLP `gas_price` field — the program reads no pool. So
the pool below is a **proxy-side input**, not an on-chain dependency.

The price source is migrating from Pyth (Hermes API) to a **Meteora cp-amm
(DAMM v2)** pool: deeper liquidity, a single `sqrt_price` read, and no external
service to self-host. Pyth (`HermesApi`) stays available as a fallback.

## Fixed price (`type: fixed`)

For chains that want a pinned gas price with no external source, `price_manager`
also accepts a `fixed` variant — no pool, no RPC, no polling:

```yaml
price_manager:
  type: fixed
  price: 0.045     # gas-token/SOL, same units as the pools' min_price/max_price
```

`price` is a fixed **rate**, not a final wei value: it flows through the same
`× reserve × 10^(decimals) × gas_price_mul` pipeline as a pool reading, so
`eth_gasPrice` keeps its usual headroom over the submit-validation floor. Tune
`gas_price_mul` if you need `eth_gasPrice` to land on a specific number.

## Pools

| Pool type | Network | Address | Pair (token_a / token_b) | Role |
|-----------|---------|---------|--------------------------|------|
| DAMM v1 (`Eo7WjKq…`) | mainnet | `5yuefgbJJpmFNK2iiYbLSpv1aZXq7F9AUKkZKErTYCvs` | USDC `EPjFWdd5…` / WSOL | legacy v1 reference |
| DAMM v1 (`Eo7WjKq…`) | devnet | `VJwzHDDkunWrRS3mDsRd2JRWTt22G5PdRLZjQhWsJga` | USDC `4zMMC9…` / WSOL | Hadrian's current gas pool |
| cp-amm / DAMM v2 (`cpamdpZC…`) | mainnet | `8Pm2kZpnxD3hoMmt4bjStX2Pw2Z9abpbHzZxMPqxPmie` | WSOL / USDC `EPjFWdd5…` | **mainnet gas source (target)** |
| cp-amm / DAMM v2 (`cpamdpZC…`) | devnet | `rLxJMgRWU43Cj4Dad9Qr23wzcQvJ4WejRtZMaDyfBCN` | WSOL / USDC `4zMMC9…` | devnet / Hadrian e2e source |

USDC mint mapping mainnet ↔ devnet: `EPjFWdd5…` ↔ `4zMMC9…` (Circle). WSOL is the
same mint on both clusters.

### How the devnet v2 pool was identified
Production v1 `5yuefgbJJ…` (USDC `EPjF`/WSOL) maps one-to-one to devnet v1
`VJwzHDDk…` (USDC `4zMMC9`/WSOL), which fixes the USDC mapping `EPjF ↔ 4zMMC9`.
Production v2 `8Pm2kZ…` is `EPjF`/WSOL, so its devnet analog is the `4zMMC9`/WSOL
cp-amm pool. Of the three such pools on devnet, `rLxJMg…` is the real one
(~75.4 WSOL + 25.5 USDC reserves; WSOL-as-token_a ordering matching production —
the other two are near-empty). Devnet pool prices are arbitrary (no arbitrage);
only the mainnet pool carries a real ~$74 SOL price. The devnet pool exists to
exercise the reader mechanics end-to-end on Hadrian.

## Reader mechanics (cp-amm)
The cp-amm `Pool` account is 1112 bytes with a fixed layout (identical across
clusters):

| Field | Offset | Type |
|-------|--------|------|
| token_a_mint | 168 | `Pubkey` |
| token_b_mint | 200 | `Pubkey` |
| sqrt_price | 456 | `u128` LE (Q64.64) |

Price of token_a in token_b: `(sqrt_price / 2^64)^2 × 10^(dec_a − dec_b)`, with
decimals read from the two mints. Verified against `8Pm2kZ…`:
`sqrt_price = 5028764307534083343`, WSOL(9)/USDC(6) → **≈ $74.32 / SOL**.

Reader: `rome-sdk/rome-meteora` (`DammV2Pool`); consumed by `MeteoraPriceManager`
via the `MeteoraDammV2Pool` price-manager config variant.

### Manipulation guard (clamp)
A single pool's marginal price can be pushed by one large swap, so the
price-manager applies an optional `min_price` / `max_price` clamp (set in the
`price_manager` config) to bound the derived gas-token price before it reaches
`eth_gasPrice`. Bounds are per-pool config; omit for no clamp.

```yaml
price_manager:
  type: meteora_damm_v2_pool
  pool_address: "8Pm2kZpnxD3hoMmt4bjStX2Pw2Z9abpbHzZxMPqxPmie"
  min_price: 40.0     # USDC per SOL floor (optional)
  max_price: 1000.0   # USDC per SOL ceiling (optional)
```

## Source of truth
The **live per-chain pool address** is registry-driven: it lives in
`rome-protocol/registry` under `chains/<id>/` and is codegen'd into the proxy's
`price_manager` config. This document is reference/rationale — the addresses here
are **not** the runtime source of truth.
