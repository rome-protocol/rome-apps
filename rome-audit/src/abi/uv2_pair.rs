//! UV2 pair (Uniswap V2, `SourceKind::Uv2Pair`) — capture §2.8, spine-driven
//! discovery (not registry-gated). P3b registered **only** the pair's own
//! `Transfer` (its LP-share ERC-20 event, §2.8: "record LP-share transfers
//! ... as a known exposure channel" — this is what P3b's discovered-source
//! walk reads to find further counterparties).
//!
//! `Transfer(address,address,uint256)` is the SAME signature (same
//! `topic0`) as `ArcToken`'s own `Transfer` — reusing `arc_token::
//! TRANSFER_TOPIC0` here registers it under a DIFFERENT `(source_kind,
//! topic0)` key (`Uv2Pair` vs `ArcToken`), which `AbiRegistry` allows (its
//! uniqueness constraint is the pair, not the topic0 alone) — and because
//! `AbiRegistry::event_names()` dedupes by NAME, this does not introduce a
//! new name for a fixture-coverage check to demand (the name "Transfer" is
//! already covered via `ArcToken`).
//!
//! P4a adds the pair's full standard Uniswap V2 event surface (`Swap`/
//! `Mint`/`Burn`/`Sync` — the DEFERRED-until-now events P3b's own module doc
//! named), verified against the canonical `UniswapV2Pair.sol` signatures:
//!
//! ```solidity
//! event Swap(address indexed sender, uint256 amount0In, uint256 amount1In,
//!     uint256 amount0Out, uint256 amount1Out, address indexed to);
//! event Mint(address indexed sender, uint256 amount0, uint256 amount1);
//! event Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to);
//! event Sync(uint112 reserve0, uint112 reserve1);
//! ```
//! `Sync`'s `uint112` args decode via `ArgType::Uint256` — a `uint112`
//! ABI-word is right-aligned in the SAME 32-byte word a `uint256` occupies
//! (this decoder has no narrower unsigned-int variant, and none is needed:
//! the recovered decimal value is byte-identical either way). `topic0` for
//! each was computed from the CANONICAL signature above (the type that
//! actually hashes), independently of the narrower-width decode choice.

use crate::abi::arc_token::TRANSFER_TOPIC0;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

use super::topic0_from_hex;

pub const SWAP_TOPIC0: [u8; 32] =
    topic0_from_hex("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");
pub const MINT_TOPIC0: [u8; 32] =
    topic0_from_hex("4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f");
pub const BURN_TOPIC0: [u8; 32] =
    topic0_from_hex("dccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496");
pub const SYNC_TOPIC0: [u8; 32] =
    topic0_from_hex("1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::Uv2Pair,
        TRANSFER_TOPIC0,
        EventDescriptor {
            event_name: "Transfer",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("from", ArgType::Address, true),
                ArgSpec::new("to", ArgType::Address, true),
                ArgSpec::new("value", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Uv2Pair,
        SWAP_TOPIC0,
        EventDescriptor {
            event_name: "Swap",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("sender", ArgType::Address, true),
                ArgSpec::new("amount0In", ArgType::Uint256, false),
                ArgSpec::new("amount1In", ArgType::Uint256, false),
                ArgSpec::new("amount0Out", ArgType::Uint256, false),
                ArgSpec::new("amount1Out", ArgType::Uint256, false),
                ArgSpec::new("to", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::Uv2Pair,
        MINT_TOPIC0,
        EventDescriptor {
            event_name: "Mint",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("sender", ArgType::Address, true),
                ArgSpec::new("amount0", ArgType::Uint256, false),
                ArgSpec::new("amount1", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Uv2Pair,
        BURN_TOPIC0,
        EventDescriptor {
            event_name: "Burn",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("sender", ArgType::Address, true),
                ArgSpec::new("amount0", ArgType::Uint256, false),
                ArgSpec::new("amount1", ArgType::Uint256, false),
                ArgSpec::new("to", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::Uv2Pair,
        SYNC_TOPIC0,
        EventDescriptor {
            event_name: "Sync",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("reserve0", ArgType::Uint256, false),
                ArgSpec::new("reserve1", ArgType::Uint256, false),
            ],
        },
    );
}
