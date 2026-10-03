//! `ArcTokenPurchase` (Arc contracts `contracts/src/ArcTokenPurchase.sol`) — P3
//! needs `PurchaseTokenUpdated` to walk the storefront's purchase-token
//! history (capture §1.2 step 2, purchase-token interval discovery).
//! Signature verified against source:
//!
//! ```solidity
//! event PurchaseTokenUpdated(address indexed newPurchaseToken);
//! ```
//!
//! P4a adds the rest of the storefront's own event surface (capture §1.3's
//! superset-capture principle) plus the standard OZ `UUPSUpgradeable`
//! `Upgraded` event, verified against source:
//!
//! ```solidity
//! event PurchaseMade(address indexed buyer, address indexed tokenContract, uint256 amount, uint256 pricePaid);
//! event TokenSaleEnabled(address indexed tokenContract, uint256 numberOfTokens, uint256 tokenPrice);
//! event TokenSaleDisabled(address indexed tokenContract);
//! event StorefrontConfigSet(address indexed tokenContract, string domain);
//! event TokenFactoryUpdated(address indexed newFactory);
//! ```

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const PURCHASE_TOKEN_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("668a2250c20049ce9de599e4808eb8685f0aa54cb49d1563548266404553cc23");
pub const PURCHASE_MADE_TOPIC0: [u8; 32] =
    topic0_from_hex("69103e55e7d412d83b3be1a3fd0908d8197fb33a5485f2b57346a9120bb1f321");
pub const TOKEN_SALE_ENABLED_TOPIC0: [u8; 32] =
    topic0_from_hex("efd79f18d51c3ec48a6fafa0646e05e45ea5e4aeec68ce1b8f61900a21be5307");
pub const TOKEN_SALE_DISABLED_TOPIC0: [u8; 32] =
    topic0_from_hex("59ed64a38b2390a57dd06250e69f1be104c45582d10a3d8273f04c2f77dff889");
pub const STOREFRONT_CONFIG_SET_TOPIC0: [u8; 32] =
    topic0_from_hex("258d321800a3233c6496a1cfd966d172f299594970268086cdc2f8a30f3b04d4");
pub const TOKEN_FACTORY_UPDATED_TOPIC0: [u8; 32] =
    topic0_from_hex("06d64f41e0c8bfe9eab59ac2d4e14dadfeee426e0ce29bcda6d82a5bf1a1c1cf");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::Storefront,
        PURCHASE_TOKEN_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "PurchaseTokenUpdated",
            projection_tag: ProjectionTag::Primary,
            args: vec![ArgSpec::new("newPurchaseToken", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Storefront,
        PURCHASE_MADE_TOPIC0,
        EventDescriptor {
            event_name: "PurchaseMade",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("buyer", ArgType::Address, true),
                ArgSpec::new("tokenContract", ArgType::Address, true),
                ArgSpec::new("amount", ArgType::Uint256, false),
                ArgSpec::new("pricePaid", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Storefront,
        TOKEN_SALE_ENABLED_TOPIC0,
        EventDescriptor {
            event_name: "TokenSaleEnabled",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("tokenContract", ArgType::Address, true),
                ArgSpec::new("numberOfTokens", ArgType::Uint256, false),
                ArgSpec::new("tokenPrice", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Storefront,
        TOKEN_SALE_DISABLED_TOPIC0,
        EventDescriptor {
            event_name: "TokenSaleDisabled",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("tokenContract", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Storefront,
        STOREFRONT_CONFIG_SET_TOPIC0,
        EventDescriptor {
            event_name: "StorefrontConfigSet",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("tokenContract", ArgType::Address, true),
                ArgSpec::new("domain", ArgType::String, false),
            ],
        },
    );
    registry.register(
        SourceKind::Storefront,
        TOKEN_FACTORY_UPDATED_TOPIC0,
        EventDescriptor {
            event_name: "TokenFactoryUpdated",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("newFactory", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Storefront,
        super::arc_token::UPGRADED_TOPIC0,
        EventDescriptor {
            event_name: "Upgraded",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("implementation", ArgType::Address, true)],
        },
    );
    // P4b-ii (capture-gap fix, additive): `ArcTokenPurchase is
    // Initializable, AccessControlUpgradeable, UUPSUpgradeable,
    // ReentrancyGuardUpgradeable` (source verified above) — `Upgraded` was
    // already registered; the rest of the OZ AccessControl+Initializable
    // surface was not. Reuse `arc_token`'s shared topic0 consts, zero new
    // literals.
    registry.register(
        SourceKind::Storefront,
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
        SourceKind::Storefront,
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
        SourceKind::Storefront,
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
        SourceKind::Storefront,
        super::arc_token::INITIALIZED_TOPIC0,
        EventDescriptor {
            event_name: "Initialized",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("version", ArgType::Uint256, false)],
        },
    );
}
