//! `GlobalSanctions` (Arc contracts `rome-contracts/src/GlobalSanctions.sol`) —
//! the Axis-2 (sanctions) module. Signatures verified against source:
//!
//! ```solidity
//! event Sanctioned(address indexed account);
//! event Unsanctioned(address indexed account);
//! event TransferScreened(address indexed from, address indexed to, uint256 amount);
//! ```
//!
//! All three are live on a Rome testnet (deployed and verified
//! 2026-08-16) — these `_TOPIC0` consts are the real
//! on-chain values, not merely a keccak of the signature string; that
//! equality is the anchor assertion in `decode::tests`.
//!
//! ## V2 surface (P4b, 2026-08-20)
//!
//! A fresh `GlobalSanctions` deployment emits a superset of
//! events, verified against source (`rome-contracts/src/GlobalSanctions.sol`):
//!
//! ```solidity
//! event SanctionedSet(address indexed account, bool sanctioned);
//! event AssetFrozenSet(address indexed asset, bool frozen);
//! event TransferScreened(address indexed asset, address indexed from, address indexed to, uint256 amount);
//! ```
//!
//! `TransferScreened` here is a DIFFERENT, 4-arg, asset-indexed event from
//! the 3-arg V1 one above — same Solidity name, a distinct `topic0` (the
//! module gained an `asset` indexed arg), registered separately below and
//! disambiguated at decode time purely by `topic0` (the registry key is
//! `(SourceKind, topic0)`, never the name).
//!
//! `GlobalSanctions is ... AccessControl` (plain constructor, non-upgradeable
//! — no `Initializable`/UUPS surface to register): its `RoleGranted` /
//! `RoleRevoked` / `RoleAdminChanged` are the same standard OZ
//! `IAccessControl` events already registered for other modules — reused
//! verbatim from `arc_token`'s shared topic0 consts (same pattern as
//! `router::register`), zero new literals.

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const SANCTIONED_TOPIC0: [u8; 32] =
    topic0_from_hex("63d72e072b8020c07d946ff140e00fdb10195e4cd045c83d7db998ad1239d101");
pub const UNSANCTIONED_TOPIC0: [u8; 32] =
    topic0_from_hex("6341cf58a5c9c850948623b3033047d31cfb1e741434141a9b4cafabe9cc9572");
pub const TRANSFER_SCREENED_TOPIC0: [u8; 32] =
    topic0_from_hex("6722dfcb37c25fad39b85791cf1db49a3f82299501ed35c38a1736963f9c23a1");

/// `keccak256("SanctionedSet(address,bool)")` — verified live (V2 module,
/// see module doc).
pub const SANCTIONED_SET_TOPIC0: [u8; 32] =
    topic0_from_hex("e120f1c2a7416d3ef8776f69c89583cdc9ab7f2c3e525f55df52c5b5ce453608");
/// `keccak256("AssetFrozenSet(address,bool)")` — verified live (V2 module,
/// see module doc).
pub const ASSET_FROZEN_SET_TOPIC0: [u8; 32] =
    topic0_from_hex("68e2297c215eb4de4ed9a376854b97df7af5350d4b0d2e489620509ce1147575");
/// `keccak256("TransferScreened(address,address,address,uint256)")` — the
/// V2, 4-arg, asset-indexed `TransferScreened`. Distinct from
/// `TRANSFER_SCREENED_TOPIC0` (V1, 3-arg) above; verified live (V2 module,
/// see module doc).
pub const TRANSFER_SCREENED_V2_TOPIC0: [u8; 32] =
    topic0_from_hex("edcf706d8177eb31edf03ec879920a41a678ebc47c659dcdd5ed35d660f8d11f");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::GlobalSanctions,
        SANCTIONED_TOPIC0,
        EventDescriptor {
            event_name: "Sanctioned",
            projection_tag: ProjectionTag::Primary,
            args: vec![ArgSpec::new("account", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::GlobalSanctions,
        UNSANCTIONED_TOPIC0,
        EventDescriptor {
            event_name: "Unsanctioned",
            projection_tag: ProjectionTag::Primary,
            args: vec![ArgSpec::new("account", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::GlobalSanctions,
        TRANSFER_SCREENED_TOPIC0,
        EventDescriptor {
            event_name: "TransferScreened",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("from", ArgType::Address, true),
                ArgSpec::new("to", ArgType::Address, true),
                ArgSpec::new("amount", ArgType::Uint256, false),
            ],
        },
    );

    // ---- V2 surface (P4b, 2026-08-20) — see module doc. ----

    registry.register(
        SourceKind::GlobalSanctions,
        SANCTIONED_SET_TOPIC0,
        EventDescriptor {
            event_name: "SanctionedSet",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("account", ArgType::Address, true),
                ArgSpec::new("sanctioned", ArgType::Bool, false),
            ],
        },
    );
    registry.register(
        SourceKind::GlobalSanctions,
        TRANSFER_SCREENED_V2_TOPIC0,
        EventDescriptor {
            event_name: "TransferScreened",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("asset", ArgType::Address, true),
                ArgSpec::new("from", ArgType::Address, true),
                ArgSpec::new("to", ArgType::Address, true),
                ArgSpec::new("amount", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::GlobalSanctions,
        ASSET_FROZEN_SET_TOPIC0,
        EventDescriptor {
            event_name: "AssetFrozenSet",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("asset", ArgType::Address, true),
                ArgSpec::new("frozen", ArgType::Bool, false),
            ],
        },
    );

    // `GlobalSanctions is ... AccessControl` — no `Initializable`/UUPS
    // surface (plain constructor, non-upgradeable). Reuses `arc_token`'s
    // shared OZ topic0 consts verbatim (same pattern as `router::register`).
    registry.register(
        SourceKind::GlobalSanctions,
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
        SourceKind::GlobalSanctions,
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
        SourceKind::GlobalSanctions,
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
}
