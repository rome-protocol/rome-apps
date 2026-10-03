//! `YieldBlacklistRestrictions` (Arc contracts
//! `contracts/src/restrictions/YieldBlacklistRestrictions.sol`) — P4a's
//! Axis-"yield restriction" module (capture §2.3; `resolve::resolver`
//! resolves it under `SourceKind::YieldBlacklist` via the
//! `YIELD_RESTRICTION_TYPE` module-swap walk). Signature verified against
//! source:
//!
//! ```solidity
//! event YieldBlacklistUpdated(address indexed account, bool isBlacklisted);
//! ```
//!
//! Plus the standard OZ surface every upgradeable, access-controlled module
//! in this codebase shares (`AccessControlUpgradeable` + `UUPSUpgradeable`)
//! — `Upgraded`/`RoleGranted`/`RoleRevoked`, same topic0s as
//! `arc_token`'s (a different `(source_kind, topic0)` key; see
//! `abi/erc20.rs`'s module doc for why that's safe and fixture-coverage-free
//! for the shared name).

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const YIELD_BLACKLIST_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("4c77449574928f5f4a39c96b8322ace61cffe0c0cfe2970d4f5e128b0ce8a0c3");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::YieldBlacklist,
        YIELD_BLACKLIST_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "YieldBlacklistUpdated",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("account", ArgType::Address, true),
                ArgSpec::new("isBlacklisted", ArgType::Bool, false),
            ],
        },
    );
    registry.register(
        SourceKind::YieldBlacklist,
        super::arc_token::UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "Upgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::YieldBlacklist,
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
        SourceKind::YieldBlacklist,
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
}
