//! Morpho Blue (singleton) — capture §2.9. P3b registered **only**
//! `CreateMarket`, the market-discovery trigger.
//!
//! ```solidity
//! // Morpho Blue IMorpho.sol / EventsLib.sol
//! event CreateMarket(Id indexed id, MarketParams marketParams);
//! struct MarketParams {
//!     address loanToken;
//!     address collateralToken;
//!     address oracle;
//!     address irm;
//!     uint256 lltv;
//! }
//! ```
//!
//! `Id` is a `bytes32` newtype — canonical ABI signature (what `topic0`
//! hashes) uses the underlying type. `MarketParams` is an all-static tuple
//! (5 fixed-width fields, no dynamic member), so it ABI-encodes as 5
//! consecutive 32-byte words with NO head/tail indirection — exactly the
//! shape this crate's existing flat, non-indexed `ArgSpec` decoder already
//! handles; no `ArgType::Tuple` variant needed.
//!
//! `topic0` independently verified via `cast keccak
//! "CreateMarket(bytes32,(address,address,address,address,uint256))"` —
//! **NOT yet cross-checked against a real on-chain Morpho log** (no live
//! fixture available to this crate yet).
//!
//! P4a adds the rest of the EventsLib surface, verified against the
//! VENDORED `permissioned-lending-spike/contracts/vendor/morpho-blue/src/libraries/EventsLib.sol`
//! (full text quoted per-event below since this file — unlike the rest of
//! this crate's ABI modules — has NO located real on-chain fixture at all
//! yet for ANY of its events beyond `CreateMarket`; every signature here is
//! transcribed directly from that vendored source, not from memory).
//!
//! **Indexed-layout verification (P4a verify-item #1) — read before
//! touching this file again.** `Withdraw`/`Borrow`/`WithdrawCollateral` have
//! `caller` NON-indexed, UNLIKE `Supply`/`Repay`/`SupplyCollateral` (which
//! index `caller`) — confirmed line-by-line against the vendored source:
//!
//! ```solidity
//! event Supply(Id indexed id, address indexed caller, address indexed onBehalf, uint256 assets, uint256 shares);              // EventsLib.sol:44 — indexed: id, caller, onBehalf
//! event Withdraw(Id indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares); // EventsLib.sol:53 — indexed: id, onBehalf, receiver (caller NOT indexed)
//! event Borrow(Id indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares);   // EventsLib.sol:69 — SAME layout as Withdraw
//! event Repay(Id indexed id, address indexed caller, address indexed onBehalf, uint256 assets, uint256 shares);               // EventsLib.sol:84 — SAME layout as Supply
//! event SupplyCollateral(Id indexed id, address indexed caller, address indexed onBehalf, uint256 assets);                    // EventsLib.sol:91 — SAME layout as Supply
//! event WithdrawCollateral(Id indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets); // EventsLib.sol:99 — SAME layout as Withdraw (caller NOT indexed)
//! event Liquidate(Id indexed id, address indexed caller, address indexed borrower, uint256 repaidAssets, uint256 repaidShares, uint256 seizedAssets, uint256 badDebtAssets, uint256 badDebtShares); // EventsLib.sol:112 — indexed: id, caller, borrower
//! event SetOwner(address indexed newOwner);                                                                                   // EventsLib.sol:13
//! event SetFee(Id indexed id, uint256 newFee);                                                                                // EventsLib.sol:18
//! event SetFeeRecipient(address indexed newFeeRecipient);                                                                     // EventsLib.sol:22
//! event EnableIrm(address indexed irm);                                                                                       // EventsLib.sol:26
//! event EnableLltv(uint256 lltv);                                                                                             // EventsLib.sol:30 — NOT indexed at all
//! event FlashLoan(address indexed caller, address indexed token, uint256 assets);                                            // EventsLib.sol:127
//! event SetAuthorization(address indexed caller, address indexed authorizer, address indexed authorized, bool newIsAuthorized); // EventsLib.sol:134 — 3 indexed
//! event IncrementNonce(address indexed caller, address indexed authorizer, uint256 usedNonce);                               // EventsLib.sol:142
//! event AccrueInterest(Id indexed id, uint256 prevBorrowRate, uint256 interest, uint256 feeShares);                          // EventsLib.sol:149 — only `id` indexed
//! ```
//!
//! `Supply`/`Repay`/`SupplyCollateral`/`Withdraw`/`Borrow`/`WithdrawCollateral`/
//! `Liquidate` register as `Primary` (the market-activity events capture
//! §2.9 cares about); the rest (`SetOwner`/`SetFee`/`SetFeeRecipient`/
//! `EnableIrm`/`EnableLltv`/`FlashLoan`/`SetAuthorization`/`IncrementNonce`/
//! `AccrueInterest`) are chain-global GOVERNANCE facts, never market-keyed
//! (no `id` arg on most of them — `SetFee`/`AccrueInterest` are the two
//! exceptions that DO carry `id`) — registered `Supporting`.

use super::topic0_from_hex;
use crate::registry::{AbiRegistry, ArgSpec, ArgType, EventDescriptor};
use crate::types::{ProjectionTag, SourceKind};

pub const CREATE_MARKET_TOPIC0: [u8; 32] =
    topic0_from_hex("ac4b2400f169220b0c0afdde7a0b32e775ba727ea1cb30b35f935cdaab8683ac");
pub const SUPPLY_TOPIC0: [u8; 32] =
    topic0_from_hex("edf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0");
pub const WITHDRAW_TOPIC0: [u8; 32] =
    topic0_from_hex("a56fc0ad5702ec05ce63666221f796fb62437c32db1aa1aa075fc6484cf58fbf");
pub const BORROW_TOPIC0: [u8; 32] =
    topic0_from_hex("570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43");
pub const REPAY_TOPIC0: [u8; 32] =
    topic0_from_hex("52acb05cebbd3cd39715469f22afbf5a17496295ef3bc9bb5944056c63ccaa09");
pub const SUPPLY_COLLATERAL_TOPIC0: [u8; 32] =
    topic0_from_hex("a3b9472a1399e17e123f3c2e6586c23e504184d504de59cdaa2b375e880c6184");
pub const WITHDRAW_COLLATERAL_TOPIC0: [u8; 32] =
    topic0_from_hex("e80ebd7cc9223d7382aab2e0d1d6155c65651f83d53c8b9b06901d167e321142");
pub const LIQUIDATE_TOPIC0: [u8; 32] =
    topic0_from_hex("a4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41");
pub const SET_OWNER_TOPIC0: [u8; 32] =
    topic0_from_hex("167d3e9c1016ab80e58802ca9da10ce5c6a0f4debc46a2e7a2cd9e56899a4fb5");
pub const SET_FEE_TOPIC0: [u8; 32] =
    topic0_from_hex("139d6f58e9a127229667c8e3b36e88890a66cfc8ab1024ddc513e189e125b75b");
pub const SET_FEE_RECIPIENT_TOPIC0: [u8; 32] =
    topic0_from_hex("2e979f80fe4d43055c584cf4a8467c55875ea36728fc37176c05acd784eb7a73");
pub const ENABLE_IRM_TOPIC0: [u8; 32] =
    topic0_from_hex("590e04cdebeccba40f566186b9746ad295a4cd358ea4fefaaea6ce79630d96c0");
pub const ENABLE_LLTV_TOPIC0: [u8; 32] =
    topic0_from_hex("297b80e7a896fad470c630f6575072d609bde997260ff3db851939405ec29139");
pub const FLASH_LOAN_TOPIC0: [u8; 32] =
    topic0_from_hex("c76f1b4fe4396ac07a9fa55a415d4ca430e72651d37d3401f3bed7cb13fc4f12");
pub const SET_AUTHORIZATION_TOPIC0: [u8; 32] =
    topic0_from_hex("d5e969f01efe921d3f766bdebad25f0a05e3f237311f56482bf132d0326309c0");
pub const INCREMENT_NONCE_TOPIC0: [u8; 32] =
    topic0_from_hex("a58af1a0c70dba0c7aa60d1a1a147ebd61000d1690a968828ac718bca927f2c7");
pub const ACCRUE_INTEREST_TOPIC0: [u8; 32] =
    topic0_from_hex("9d9bd501d0657d7dfe415f779a620a62b78bc508ddc0891fbbd8b7ac0f8fce87");

pub(crate) fn register(registry: &mut AbiRegistry) {
    registry.register(
        SourceKind::Morpho,
        CREATE_MARKET_TOPIC0,
        EventDescriptor {
            event_name: "CreateMarket",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("loanToken", ArgType::Address, false),
                ArgSpec::new("collateralToken", ArgType::Address, false),
                ArgSpec::new("oracle", ArgType::Address, false),
                ArgSpec::new("irm", ArgType::Address, false),
                ArgSpec::new("lltv", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        SUPPLY_TOPIC0,
        EventDescriptor {
            event_name: "Supply",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("onBehalf", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
                ArgSpec::new("shares", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        WITHDRAW_TOPIC0,
        EventDescriptor {
            event_name: "Withdraw",
            projection_tag: ProjectionTag::Primary,
            // caller is NOT indexed here (unlike Supply) — verified above.
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, false),
                ArgSpec::new("onBehalf", ArgType::Address, true),
                ArgSpec::new("receiver", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
                ArgSpec::new("shares", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        BORROW_TOPIC0,
        EventDescriptor {
            event_name: "Borrow",
            projection_tag: ProjectionTag::Primary,
            // Same layout as Withdraw — caller NOT indexed.
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, false),
                ArgSpec::new("onBehalf", ArgType::Address, true),
                ArgSpec::new("receiver", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
                ArgSpec::new("shares", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        REPAY_TOPIC0,
        EventDescriptor {
            event_name: "Repay",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("onBehalf", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
                ArgSpec::new("shares", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        SUPPLY_COLLATERAL_TOPIC0,
        EventDescriptor {
            event_name: "SupplyCollateral",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("onBehalf", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        WITHDRAW_COLLATERAL_TOPIC0,
        EventDescriptor {
            event_name: "WithdrawCollateral",
            projection_tag: ProjectionTag::Primary,
            // Same layout as Withdraw — caller NOT indexed.
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, false),
                ArgSpec::new("onBehalf", ArgType::Address, true),
                ArgSpec::new("receiver", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        LIQUIDATE_TOPIC0,
        EventDescriptor {
            event_name: "Liquidate",
            projection_tag: ProjectionTag::Primary,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("borrower", ArgType::Address, true),
                ArgSpec::new("repaidAssets", ArgType::Uint256, false),
                ArgSpec::new("repaidShares", ArgType::Uint256, false),
                ArgSpec::new("seizedAssets", ArgType::Uint256, false),
                ArgSpec::new("badDebtAssets", ArgType::Uint256, false),
                ArgSpec::new("badDebtShares", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        SET_OWNER_TOPIC0,
        EventDescriptor {
            event_name: "SetOwner",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("newOwner", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Morpho,
        SET_FEE_TOPIC0,
        EventDescriptor {
            event_name: "SetFee",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("newFee", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        SET_FEE_RECIPIENT_TOPIC0,
        EventDescriptor {
            event_name: "SetFeeRecipient",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("newFeeRecipient", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Morpho,
        ENABLE_IRM_TOPIC0,
        EventDescriptor {
            event_name: "EnableIrm",
            projection_tag: ProjectionTag::Supporting,
            args: vec![ArgSpec::new("irm", ArgType::Address, true)],
        },
    );
    registry.register(
        SourceKind::Morpho,
        ENABLE_LLTV_TOPIC0,
        EventDescriptor {
            event_name: "EnableLltv",
            projection_tag: ProjectionTag::Supporting,
            // NOT indexed at all — no non-indexed-only assertion needed.
            args: vec![ArgSpec::new("lltv", ArgType::Uint256, false)],
        },
    );
    registry.register(
        SourceKind::Morpho,
        FLASH_LOAN_TOPIC0,
        EventDescriptor {
            event_name: "FlashLoan",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("token", ArgType::Address, true),
                ArgSpec::new("assets", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        SET_AUTHORIZATION_TOPIC0,
        EventDescriptor {
            event_name: "SetAuthorization",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("authorizer", ArgType::Address, true),
                ArgSpec::new("authorized", ArgType::Address, true),
                ArgSpec::new("newIsAuthorized", ArgType::Bool, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        INCREMENT_NONCE_TOPIC0,
        EventDescriptor {
            event_name: "IncrementNonce",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("caller", ArgType::Address, true),
                ArgSpec::new("authorizer", ArgType::Address, true),
                ArgSpec::new("usedNonce", ArgType::Uint256, false),
            ],
        },
    );
    registry.register(
        SourceKind::Morpho,
        ACCRUE_INTEREST_TOPIC0,
        EventDescriptor {
            event_name: "AccrueInterest",
            projection_tag: ProjectionTag::Supporting,
            args: vec![
                ArgSpec::new("id", ArgType::Bytes32, true),
                ArgSpec::new("prevBorrowRate", ArgType::Uint256, false),
                ArgSpec::new("interest", ArgType::Uint256, false),
                ArgSpec::new("feeShares", ArgType::Uint256, false),
            ],
        },
    );
}
