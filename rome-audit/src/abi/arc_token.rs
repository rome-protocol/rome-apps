//! `ArcToken` (Arc contracts `contracts/src/ArcToken.sol`) — P0 covers exactly
//! the two events the plan scopes: the inherited ERC-20 `Transfer` (proves
//! two-indexed-plus-data decode) and `AccessControlUpgradeable`'s
//! `RoleGranted` (proves a known-but-non-primary event lands as
//! `Supporting`, never dropped). Both are OpenZeppelin-standard signatures,
//! verified against the vendored interfaces:
//!
//! ```solidity
//! // openzeppelin-contracts-upgradeable/contracts/token/ERC20/IERC20.sol
//! event Transfer(address indexed from, address indexed to, uint256 value);
//! // openzeppelin-contracts-upgradeable/contracts/access/IAccessControl.sol
//! event RoleGranted(bytes32 indexed role, address indexed account, address indexed sender);
//! ```
//!
//! `RoleGranted` indexes ALL THREE args — no non-indexed data at all, which
//! is exactly why it's useful as the "indexed-only" decode fixture.
//!
//! P3 adds the two resolution-history events (capture §1.2's event-driven
//! fixed-point walk) — signatures verified against the vendored source
//! (`contracts/src/ArcToken.sol`):
//!
//! ```solidity
//! event SpecificRestrictionModuleSet(bytes32 indexed typeId, address indexed moduleAddress);
//! event YieldTokenUpdated(address indexed newYieldToken);
//! ```
//! Both args are indexed on `SpecificRestrictionModuleSet` — no non-indexed
//! `data` at all, same decode shape as `RoleGranted`.
//!
//! P4a adds the rest of `ArcToken.sol`'s own event surface (capture §1.3's
//! superset-capture principle) plus the standard OZ
//! `AccessControlUpgradeable`/`UUPSUpgradeable`/`IERC20`/`Initializable`
//! events every module in this codebase inherits — declared HERE as the
//! single source of truth (`arc_token::UPGRADED_TOPIC0` etc.), reused by
//! `whitelist_restrictions`/`storefront`/`factory`/`yield_blacklist` under
//! their OWN `SourceKind` (a different `(source_kind, topic0)` key each
//! time — see `abi/erc20.rs`'s module doc for why that's safe):
//!
//! ```solidity
//! event YieldDistributed(uint256 amount, address indexed token);      // ArcToken.sol:72
//! event TokenNameUpdated(string oldName, string newName);              // ArcToken.sol:74
//! event TokenURIUpdated(string newTokenURI);                           // ArcToken.sol:75
//! event SymbolUpdated(string oldSymbol, string newSymbol);             // ArcToken.sol:76
//! event Upgraded(address indexed implementation);                     // IERC1967.sol
//! event RoleRevoked(bytes32 indexed role, address indexed account, address indexed sender); // IAccessControl.sol
//! event RoleAdminChanged(bytes32 indexed role, bytes32 indexed previousAdminRole, bytes32 indexed newAdminRole); // IAccessControl.sol
//! event Approval(address indexed owner, address indexed spender, uint256 value); // IERC20.sol
//! event Initialized(uint64 version);                                  // Initializable.sol — NOT indexed
//! ```

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const TRANSFER_TOPIC0: [u8; 32] =
    topic0_from_hex("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
pub const ROLE_GRANTED_TOPIC0: [u8; 32] =
    topic0_from_hex("2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d");
pub const SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0: [u8; 32] =
    topic0_from_hex("e5d41ffde12968cb577153d35b775280c377967d3bcb834268d42e7e687ca312");
pub const YIELD_TOKEN_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("f94eb881bab685626983a1af865d32af77acd21bea663ae50e2d92c5ba9daeaf");
pub const YIELD_DISTRIBUTED_TOPIC0: [u8; 32] =
    topic0_from_hex("464b8a29d43599ace210dd9e04279148ebcfd132907c4e35b77835a05a8e2879");
pub const TOKEN_NAME_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("762b25d500491264178acdce1980fd11ed12ac2c6a25eb3a15759a9f34fa82c2");
pub const TOKEN_URI_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("3d25540fafb277423a5c9552c25096f7c0c69178c044fd61c947046f08fb09ca");
pub const SYMBOL_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("e539f7c7cd6b523196945e98454143f55e93314cc40672ad65628a7df42d483f");
/// `Upgraded(address)` — `IERC1967.sol`, shared by every UUPS-upgradeable
/// module in this codebase (reused under other `SourceKind`s).
pub const UPGRADED_TOPIC0: [u8; 32] =
    topic0_from_hex("bc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b");
/// `RoleRevoked(bytes32,address,address)` — `IAccessControl.sol`, shared.
pub const ROLE_REVOKED_TOPIC0: [u8; 32] =
    topic0_from_hex("f6391f5c32d9c69d2a47ea670b442974b53935d1edc7fd64eb21e047a839171b");
/// `RoleAdminChanged(bytes32,bytes32,bytes32)` — `IAccessControl.sol`, shared.
pub const ROLE_ADMIN_CHANGED_TOPIC0: [u8; 32] =
    topic0_from_hex("bd79b86ffe0ab8e8776151514217cd7cacd52c909f66475c3af44e129f0b00ff");
/// `Approval(address,address,uint256)` — `IERC20.sol`, shared.
pub const APPROVAL_TOPIC0: [u8; 32] =
    topic0_from_hex("8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925");
/// `Initialized(uint64)` — `Initializable.sol`, shared.
pub const INITIALIZED_TOPIC0: [u8; 32] =
    topic0_from_hex("c7f505b2f371ae2175ee4913f4499e1f2633a7b5936321eed1cdaeb6115181d2");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::ArcToken,
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
        SourceKind::ArcToken,
        ROLE_GRANTED_TOPIC0,
        EventDescriptor {
            event_name: "RoleGranted",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("role", ArgType::Bytes32, true),
                ArgSpec::new("account", ArgType::Address, true),
                ArgSpec::new("sender", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        SPECIFIC_RESTRICTION_MODULE_SET_TOPIC0,
        EventDescriptor {
            event_name: "SpecificRestrictionModuleSet",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("typeId", ArgType::Bytes32, true),
                ArgSpec::new("moduleAddress", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        YIELD_TOKEN_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "YieldTokenUpdated",
            projection_tag: ProjectionTag::Primary,
            args: vec![ArgSpec::new("newYieldToken", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        YIELD_DISTRIBUTED_TOPIC0,
        EventDescriptor {
            event_name: "YieldDistributed",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("amount", ArgType::Uint256, false),
                ArgSpec::new("token", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "Upgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        ROLE_REVOKED_TOPIC0,
        EventDescriptor {
            event_name: "RoleRevoked",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("role", ArgType::Bytes32, true),
                ArgSpec::new("account", ArgType::Address, true),
                ArgSpec::new("sender", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        ROLE_ADMIN_CHANGED_TOPIC0,
        EventDescriptor {
            event_name: "RoleAdminChanged",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("role", ArgType::Bytes32, true),
                ArgSpec::new("previousAdminRole", ArgType::Bytes32, true),
                ArgSpec::new("newAdminRole", ArgType::Bytes32, true),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        APPROVAL_TOPIC0,
        EventDescriptor {
            event_name: "Approval",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("owner", ArgType::Address, true),
                ArgSpec::new("spender", ArgType::Address, true),
                ArgSpec::new("value", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        INITIALIZED_TOPIC0,
        EventDescriptor {
            event_name: "Initialized",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("version", ArgType::Uint256, false)],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        TOKEN_NAME_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "TokenNameUpdated",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("oldName", ArgType::String, false),
                ArgSpec::new("newName", ArgType::String, false),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        SYMBOL_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "SymbolUpdated",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("oldSymbol", ArgType::String, false),
                ArgSpec::new("newSymbol", ArgType::String, false),
            ],
        },
    );
    registry.register(
        SourceKind::ArcToken,
        TOKEN_URI_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "TokenURIUpdated",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("newTokenURI", ArgType::String, false)],
        },
    );
}
