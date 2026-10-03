# CLI

Command-line tool for the Rome EVM program on Solana: register rollups, deposit funds,
query EVM state, manage treasury accounts and manage the Proxy's payer resources.

```text
cli --program-id <PROGRAM_ID> [--chain-id <CHAIN_ID>] --url <URL> [--keypair <KEYPAIR>] <COMMAND>
```

## Global options

| Flag | Description |
|---|---|
| `-p, --program-id <PROGRAM_ID>` | Base58 address of the Rome EVM program. |
| `-c, --chain-id <CHAIN_ID>` | Rollup chain ID. Required by every command except `get-rollups`, `get-program-build-info` and `alt`. |
| `-u, --url <URL>` | Solana JSON-RPC endpoint, e.g. `http://localhost:8899`. |
| `-k, --keypair <KEYPAIR>` | Path to a Solana keypair file. Required for commands that sign. `reg-rollup` falls back to the default keypair from the Solana CLI config. |

## Commands

| Command | Arguments | Signs | Description |
|---|---|---|---|
| `reg-rollup` | `<IS_SINGLE_STATE> [MINT]` | registry authority | Register a rollup. `IS_SINGLE_STATE` is `true` for single-state rollups. `MINT` is an optional SPL mint used as the rollup gas token; omit it for SOL gas. |
| `deposit` | `<ADDRESS> <BALANCE>` | depositor | Deposit funds to an EVM address. `BALANCE` is in wei (see below). |
| `get-balance` | `<ADDRESS>` | — | EVM balance of an address. |
| `get-code` | `<ADDRESS>` | — | Deployed contract bytecode. |
| `get-storage-at` | `<ADDRESS> <SLOT>` | — | Contract storage slot. |
| `get-transaction-count` | `<ADDRESS>` | — | Account nonce. |
| `get-rollups` | — | — | List registered rollups. |
| `get-program-build-info` | — | — | Build information of the Rome EVM program. |
| `create-treasure` | — | authority | Create the treasury accounts for the rollup. |
| `join-treasure` | — | authority | Move treasury balances to the upgrade authority's associated token account. |
| `get-treasure-balance` | — | — | Balances of the treasury accounts. |
| `resources` | `<PROXY_CONFIG>` | — | Show the payer resources defined in a Proxy config file. |
| `allocate-resources` | `<PROXY_CONFIG>` | payers | Allocate on-chain resources (holder accounts) for the payers in a Proxy config. |
| `deallocate-resources` | `<PROXY_CONFIG>` | payers | Release those resources and reclaim lamports to the payers. |
| `alt health` | `<MANIFEST>` | — | Maintenance for legacy persistent address lookup tables: compare each table's on-chain contents with its recorded hash. |
| `alt retire` | `<TABLE>` | keypair | Deactivate a legacy lookup table so it can be closed and its rent reclaimed after the cooldown. |

Run `cli <COMMAND> --help` for details on any command.

### Deposit amounts

The deposit path depends on the rollup's gas token:

- **SOL-gas rollup** (no mint): native SOL moves from the signer's wallet to the Rome EVM
  wallet PDA. `BALANCE` must be a multiple of `10^9` (1 SOL = 1 rollup token).
- **SPL-gas rollup**: the configured SPL token moves from the signer's associated token
  account to the gas-pool account. `BALANCE` must be a multiple of `10^(18 - decimals)`,
  for example `10^12` for a 6-decimal token.

## Examples

```sh
PROGRAM_ID=<ROME_EVM_PROGRAM_ID>
RPC=http://localhost:8899

# Register a single-state rollup that uses an SPL token for gas
cli -p $PROGRAM_ID -c 1001 -u $RPC -k ./keys/registry-authority.json \
  reg-rollup true <MINT_PUBKEY>

# Register a single-state rollup that uses SOL for gas
cli -p $PROGRAM_ID -c 1001 -u $RPC -k ./keys/registry-authority.json \
  reg-rollup true

# Deposit 1 token (10^18 wei) on a SOL-gas rollup
cli -p $PROGRAM_ID -c 1001 -u $RPC -k ./keys/user.json \
  deposit 0xe235b9caf55b58863Ae955A372e49362b0f93726 1000000000000000000

# Queries
cli -p $PROGRAM_ID -c 1001 -u $RPC get-balance 0x229E93198d584C397DFc40024d1A3dA10B73aB32
cli -p $PROGRAM_ID -c 1001 -u $RPC get-storage-at 0x229E93198d584C397DFc40024d1A3dA10B73aB32 0
cli -p $PROGRAM_ID -u $RPC get-rollups
cli -p $PROGRAM_ID -u $RPC get-program-build-info

# Treasury
cli -p $PROGRAM_ID -c 1001 -u $RPC -k ./keys/authority.json create-treasure
cli -p $PROGRAM_ID -c 1001 -u $RPC -k ./keys/authority.json join-treasure
cli -p $PROGRAM_ID -c 1001 -u $RPC get-treasure-balance

# Payer resources (reads the payers from a Proxy config file)
cli -p $PROGRAM_ID -c 1001 -u $RPC resources ./cfg/proxy-config.yml
cli -p $PROGRAM_ID -c 1001 -u $RPC allocate-resources ./cfg/proxy-config.yml
cli -p $PROGRAM_ID -c 1001 -u $RPC deallocate-resources ./cfg/proxy-config.yml
```

## Docker

The `romeprotocol/rome-apps` image includes the CLI at `/opt/cli`.
[`docker-compose.yml`](docker-compose.yml) contains ready-made services for the read-only
and resource commands. Set `PROGRAM_ID`, `CHAIN_ID` and `SOLANA_RPC` before running them:

```sh
PROGRAM_ID=<ROME_EVM_PROGRAM_ID> CHAIN_ID=1001 SOLANA_RPC=http://localhost:8899 \
  docker compose -f cli/docker-compose.yml run --rm get_rollups
```

## Building on macOS

The CLI links against `libpq`. If the linker cannot find it, install it with Homebrew and
export `LIBRARY_PATH=/opt/homebrew/opt/libpq/lib` (and `DYLD_LIBRARY_PATH` at run time).
