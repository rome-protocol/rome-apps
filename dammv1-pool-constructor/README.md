# Meteora DAMM v1 Pool Creator (devnet)

A small TypeScript script that creates a **Meteora DAMM v1 constant-product pool** on
**Solana devnet** with the Meteora TypeScript SDK, without using the Meteora UI. Rome uses
DAMM v1 pools as a gas-price source (see the Proxy `price_manager` setting), so this script
is handy for setting up test chains.

This directory is not part of the Cargo workspace.

## What it does

1. Connects to the Solana RPC in `RPC_URL` (default `https://api.devnet.solana.com`).
2. Loads the payer wallet from `SECRET_KEY_JSON`.
3. Derives the DAMM v1 pool PDA from the token pair and config.
4. Builds the pool-creation transactions using an existing DAMM v1 config account.
5. Sends the transactions in order, refreshing the recent blockhash before each one.

The script does **not** create a Meteora config account. You must supply an existing,
valid DAMM v1 devnet config in `CONFIG_PUBKEY`; an invalid config, or one owned by a
different program, makes pool creation fail.

## Requirements

- Node.js 18+ and npm
- A devnet wallet with enough SOL for rent and fees
- Two devnet token mints, and balances of both in the wallet
- A valid Meteora DAMM v1 devnet `CONFIG_PUBKEY`

## Setup

```sh
cd dammv1-pool-constructor
npm install
```

Create a `.env` file (it is ignored by git; never commit a real key):

```env
RPC_URL=https://api.devnet.solana.com
SECRET_KEY_JSON=[1,2,3,...]
TOKEN_A_MINT=So11111111111111111111111111111111111111112
TOKEN_B_MINT=<YOUR_DEVNET_TOKEN_MINT>
TOKEN_A_AMOUNT=1000000
TOKEN_B_AMOUNT=5000000
CONFIG_PUBKEY=<DAMM_V1_DEVNET_CONFIG>
SKIP_PRE_FLIGHT=false
```

| Variable | Required | Description |
|---|---|---|
| `RPC_URL` | no | Solana RPC endpoint. Defaults to public devnet. |
| `SECRET_KEY_JSON` | yes | Payer secret key as a JSON byte array (the format of a Solana keypair file). |
| `TOKEN_A_MINT` / `TOKEN_B_MINT` | yes | Mint addresses of the two pool tokens. |
| `TOKEN_A_AMOUNT` / `TOKEN_B_AMOUNT` | yes | Initial liquidity in **raw base units** (for a 6-decimal token, 1.5 tokens = `1500000`). |
| `CONFIG_PUBKEY` | yes | Existing DAMM v1 config account on devnet, for example `7kXYNBXZ4wQH87Da4NLqneoEq4oCopLgSqGQZCy1s28w`. |
| `SKIP_PRE_FLIGHT` | no | `true` to skip preflight simulation. Default `false`. |

## Running

```sh
npm run dev      # run the TypeScript source directly with ts-node
# or
npm run build    # compile to dist/
npm start        # run dist/create-dammv1-pool-devnet.js
```

A successful run prints the payer, program ID, mints, config, pool PDA and amounts, then
one signature per transaction:

```text
Pool PDA:        ...
Prepared 2 transaction(s)
tx[0] = ...
tx[1] = ...

Done.
```

The SDK may return more than one transaction for pool creation; this is expected. The
pool PDA is derived deterministically from the mint pair and config, and the SDK may
normalize mint order internally.

## Troubleshooting

| Symptom | Check |
|---|---|
| Simulation failure, custom program error or account constraint error | `CONFIG_PUBKEY` is a valid DAMM v1 devnet config. |
| Insufficient funds | Wallet SOL balance (`solana airdrop 2 <ADDRESS> --url devnet`) and balances of both tokens. |
| Wrong pool liquidity | Amounts must be raw integers, not decimal UI values. |
| Invalid mint | Both mints exist on devnet. |
| `Blockhash not found` or timeouts | Retry, or switch to a more reliable devnet RPC. |
| Only some transactions landed | Inspect the printed signatures in a devnet explorer. |

## Scope

This is a lightweight automation example for local experimentation, testing and scripted
devnet setup. It is not a production deployment tool: it has minimal validation, no
balance pre-checks and simple retry handling.

## License

Covered by the repository [LICENSE](../LICENSE).
