//! `ArcTokenFactoryV2` (Arc contracts `rome-contracts/src/ArcTokenFactoryV2.sol`)
//! — P3 needs exactly `TokenRegistered`, the onboarding trigger (capture
//! spec §2.7 / Hard Rule #3) resolution uses to find a token's own
//! registration block (the immutable router's `from_block` anchor, since
//! the router itself has no setter and never emits a "registered at" event
//! of its own). Signature verified against source:
//!
//! ```solidity
//! event TokenRegistered(address indexed token, address indexed implementation);
//! ```
//!
//! P4a adds the INHERITED V1 surface (`ArcTokenFactoryV2 is
//! ArcTokenFactory` — Arc contracts `contracts/src/ArcTokenFactory.sol`), verified
//! against source:
//!
//! ```solidity
//! event TokenCreated(address indexed tokenAddress, address indexed owner, address indexed implementation,
//!     string name, string symbol, string tokenUri, uint8 decimals);
//! event ModuleLinked(address indexed tokenAddress, address indexed moduleAddress, bytes32 indexed moduleType);
//! event ImplementationWhitelisted(address indexed implementation);
//! event ImplementationRemoved(address indexed implementation);
//! event TokenUpgraded(address indexed token, address indexed newImplementation);
//! ```
//! `decimals` (`uint8`) decodes via `ArgType::Uint256` — a `uint8` ABI-word
//! is right-aligned the SAME way a `uint256` is, so the recovered decimal
//! value is identical; only the Solidity type NAME differs (irrelevant to
//! this decoder, which has no narrower unsigned-int variant).

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const TOKEN_REGISTERED_TOPIC0: [u8; 32] =
    topic0_from_hex("487c37289624c10056468f1f98ebffbad01edce11374975179672e32e2543bf0");
pub const TOKEN_CREATED_TOPIC0: [u8; 32] =
    topic0_from_hex("87432a53b4bacb04ae99226b6316d9930f169c481d75cf5b513f4eccc485b1db");
pub const MODULE_LINKED_TOPIC0: [u8; 32] =
    topic0_from_hex("0540e31d60db77ecc418386b102018f04fd47bd4815a8ef97a3c3c9de8082613");
pub const IMPLEMENTATION_WHITELISTED_TOPIC0: [u8; 32] =
    topic0_from_hex("489c65b6ac39f6046967b57c184dcd1290e0e48217c30c899c74fddee8beecad");
pub const IMPLEMENTATION_REMOVED_TOPIC0: [u8; 32] =
    topic0_from_hex("af23121e2402485071dadf421078b368d7b67e54cabcc81540563c5d6bf1a4c3");
pub const TOKEN_UPGRADED_TOPIC0: [u8; 32] =
    topic0_from_hex("cd23a18833879e1f289f88200345417e6288fcb73ad4cdfdbf7759d735444ae5");

pub(crate) fn register(registry: &mut AbiRegistry) {
    // P4b-ii (capture-gap fix, additive): `ArcTokenFactory is Initializable,
    // AccessControlUpgradeable, UUPSUpgradeable` (source verified above) —
    // reuse `arc_token`'s shared topic0 consts, zero new literals.
    registry.register(
        SourceKind::Factory,
        super::arc_token::ROLE_GRANTED_TOPIC0,
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
        SourceKind::Factory,
        super::arc_token::ROLE_REVOKED_TOPIC0,
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
        SourceKind::Factory,
        super::arc_token::ROLE_ADMIN_CHANGED_TOPIC0,
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
        SourceKind::Factory,
        super::arc_token::UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "Upgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Factory,
        super::arc_token::INITIALIZED_TOPIC0,
        EventDescriptor {
            event_name: "Initialized",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("version", ArgType::Uint256, false)],
        },
    );
    registry.register(
        SourceKind::Factory,
        TOKEN_REGISTERED_TOPIC0,
        EventDescriptor {
            event_name: "TokenRegistered",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("token", ArgType::Address, true),
                ArgSpec::new("implementation", ArgType::Address, true),
            ],
        },
    );
    registry.register(
        SourceKind::Factory,
        TOKEN_CREATED_TOPIC0,
        EventDescriptor {
            event_name: "TokenCreated",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("tokenAddress", ArgType::Address, true),
                ArgSpec::new("owner", ArgType::Address, true),
                ArgSpec::new("implementation", ArgType::Address, true),
                ArgSpec::new("name", ArgType::String, false),
                ArgSpec::new("symbol", ArgType::String, false),
                ArgSpec::new("tokenUri", ArgType::String, false),
                ArgSpec::new("decimals", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Factory,
        MODULE_LINKED_TOPIC0,
        EventDescriptor {
            event_name: "ModuleLinked",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("tokenAddress", ArgType::Address, true),
                ArgSpec::new("moduleAddress", ArgType::Address, true),
                ArgSpec::new("moduleType", ArgType::Bytes32, true),
            ],
        },
    );
    registry.register(
        SourceKind::Factory,
        IMPLEMENTATION_WHITELISTED_TOPIC0,
        EventDescriptor {
            event_name: "ImplementationWhitelisted",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Factory,
        IMPLEMENTATION_REMOVED_TOPIC0,
        EventDescriptor {
            event_name: "ImplementationRemoved",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Factory,
        TOKEN_UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "TokenUpgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("token", ArgType::Address, true),
                ArgSpec::new("newImplementation", ArgType::Address, true),
            ],
        },
    );
}
