//! `RestrictionsRouter` (Arc contracts `contracts/src/restrictions/RestrictionsRouter.sol`)
//! — added to cover `ModuleTypeRegistered`, the router-registration event that
//! anchors a router's `router_sanctions_epoch` (IMPL-PLAN §11). Signature
//! verified against source:
//!
//! ```solidity
//! event ModuleTypeRegistered(bytes32 indexed typeId, bool isGlobal, address globalImplementation);
//! ```
//!
//! Verified against a real on-chain `GLOBAL_SANCTIONS` registration by the
//! router on a Rome testnet.
//!
//! `ModuleTypeRemoved` / `GlobalImplementationUpdated` (added for Tier-2's
//! `router_sanctions_epoch`, IMPL-PLAN §5) are the epoch-CLOSING signals for
//! the same table `ModuleTypeRegistered` opens. Verified against source
//! (`contracts/src/restrictions/RestrictionsRouter.sol:30-32`,
//! Arc contracts source):
//!
//! ```solidity
//! event ModuleTypeRemoved(bytes32 indexed typeId);
//! event GlobalImplementationUpdated(bytes32 indexed typeId, address indexed newGlobalImplementation);
//! ```

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const MODULE_TYPE_REGISTERED_TOPIC0: [u8; 32] =
    topic0_from_hex("cc4abcb2a030ec3e5fd29297416c74497d3225bdfeeac99e1f918ae95da249f9");
pub const MODULE_TYPE_REMOVED_TOPIC0: [u8; 32] =
    topic0_from_hex("dff785f41263b72f976d1eed0524dd42f4e07d938b6d8591800435d9a439cade");
pub const GLOBAL_IMPLEMENTATION_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("13de69f292a4fc57344c45d3be3f70addbd82b90ae265191b774e72e1d95f7b6");

/// `keccak256("GLOBAL_SANCTIONS")` — `RestrictionTypes.GLOBAL_SANCTIONS_TYPE`
/// (`contracts/src/restrictions/RestrictionTypes.sol:13`). The `typeId` a
/// `router_sanctions_epoch` build filters `ModuleTypeRegistered`/
/// `ModuleTypeRemoved`/`GlobalImplementationUpdated` rows down to — those
/// three events fire for every registered module type, not just sanctions.
pub const GLOBAL_SANCTIONS_TYPE: [u8; 32] =
    topic0_from_hex("7bc1beee0f96b04bb0683af043d6121adcb90bc1a247e3b62e71cc41fdc8428f");

pub(crate) fn register(registry: &mut AbiRegistry) {
    // P4b-ii (capture-gap fix, additive): `RestrictionsRouter is
    // Initializable, AccessControlUpgradeable, UUPSUpgradeable` (source
    // verified above) — its own OZ AccessControl+UUPS surface was never
    // registered under `SourceKind::Router` even though `arc_token`'s
    // shared topic0 consts already cover it. Reused verbatim, zero new
    // literals.
    registry.register(
        SourceKind::Router,
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
        SourceKind::Router,
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
        SourceKind::Router,
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
        SourceKind::Router,
        super::arc_token::UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "Upgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Router,
        super::arc_token::INITIALIZED_TOPIC0,
        EventDescriptor {
            event_name: "Initialized",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("version", ArgType::Uint256, false)],
        },
    );
    registry.register(
        SourceKind::Router,
        MODULE_TYPE_REGISTERED_TOPIC0,
        EventDescriptor {
            event_name: "ModuleTypeRegistered",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("typeId", ArgType::Bytes32, true),
                ArgSpec::new("isGlobal", ArgType::Bool, false),
                ArgSpec::new("globalImplementation", ArgType::Address, false),
            ],
        },
    );
    registry.register(
        SourceKind::Router,
        MODULE_TYPE_REMOVED_TOPIC0,
        EventDescriptor {
            event_name: "ModuleTypeRemoved",
            projection_tag: ProjectionTag::Primary,
            args: vec![ArgSpec::new("typeId", ArgType::Bytes32, true)],
        },
    );
    registry.register(
        SourceKind::Router,
        GLOBAL_IMPLEMENTATION_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "GlobalImplementationUpdated",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("typeId", ArgType::Bytes32, true),
                ArgSpec::new("newGlobalImplementation", ArgType::Address, true),
            ],
        },
    );
}
