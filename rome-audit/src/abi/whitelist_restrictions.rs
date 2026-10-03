//! `WhitelistRestrictions` (Arc contracts
//! `contracts/src/restrictions/WhitelistRestrictions.sol`) — today's shipped
//! Axis-1 (securities-gating) module, registered under `SourceKind::Axis1Module`
//! (IMPL-PLAN §"conventions": capture the ABI of the *resolved* module, not
//! a fixed one — `WhitelistRestrictions` is that resolved module today).
//! Signatures verified against source:
//!
//! ```solidity
//! event WhitelistStatusChanged(address indexed account, bool isWhitelisted);
//! event TransfersRestrictionToggled(bool transfersAllowed);
//! event AddedToWhitelist(address indexed account);      // P4a
//! event RemovedFromWhitelist(address indexed account);  // P4a
//! ```
//!
//! P4a also adds the standard OZ `AccessControlUpgradeable`/
//! `UUPSUpgradeable` surface (`RoleGranted`/`RoleRevoked`/`Upgraded`) —
//! reusing `arc_token`'s topic0 constants under THIS module's own
//! `SourceKind` (see `abi/erc20.rs`'s module doc for why that's safe).

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const WHITELIST_STATUS_CHANGED_TOPIC0: [u8; 32] =
    topic0_from_hex("8daaf060c3306c38e068a75c054bf96ecd85a3db1252712c4d93632744c42e0d");
pub const TRANSFERS_RESTRICTION_TOGGLED_TOPIC0: [u8; 32] =
    topic0_from_hex("70a23fe37c63b4aecb5c585cbcc7e044a1601e38986a22eaf1932b87030ba097");
pub const ADDED_TO_WHITELIST_TOPIC0: [u8; 32] =
    topic0_from_hex("a850ae9193f515cbae8d35e8925bd2be26627fc91bce650b8652ed254e9cab03");
pub const REMOVED_FROM_WHITELIST_TOPIC0: [u8; 32] =
    topic0_from_hex("cdd2e9b91a56913d370075169cefa1602ba36be5301664f752192bb1709df757");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::Axis1Module,
        WHITELIST_STATUS_CHANGED_TOPIC0,
        EventDescriptor {
            event_name: "WhitelistStatusChanged",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("account", ArgType::Address, true),
                ArgSpec::new("isWhitelisted", ArgType::Bool, false),
            ],
        },
    );
    registry.register(
        SourceKind::Axis1Module,
        TRANSFERS_RESTRICTION_TOGGLED_TOPIC0,
        EventDescriptor {
            event_name: "TransfersRestrictionToggled",
            projection_tag: ProjectionTag::Primary,
            args: vec![ArgSpec::new("transfersAllowed", ArgType::Bool, false)],
        },
    );
    registry.register(
        SourceKind::Axis1Module,
        ADDED_TO_WHITELIST_TOPIC0,
        EventDescriptor {
            event_name: "AddedToWhitelist",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("account", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Axis1Module,
        REMOVED_FROM_WHITELIST_TOPIC0,
        EventDescriptor {
            event_name: "RemovedFromWhitelist",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("account", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Axis1Module,
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
        SourceKind::Axis1Module,
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
        SourceKind::Axis1Module,
        super::arc_token::UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "Upgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
}
