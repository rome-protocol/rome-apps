use {
    jsonrpsee::types::{error::{CALL_EXECUTION_FAILED_CODE, INTERNAL_ERROR_CODE}, ErrorObjectOwned},
    rome_sdk::rome_evm_client::{
        error::RomeEvmError, rome_evm::{error::RomeProgramError, state::aux::revert_msg,},
    },
    solana_client::client_error::ClientError,
    thiserror::Error,
};

/// Standard "limit exceeded" JSON-RPC code (Infura/Alchemy/geth) for
/// eth_getLogs queries that match too many results or span too many blocks.
/// Client libraries recognize it as the paginate-and-retry signal.
pub const LOGS_LIMIT_EXCEEDED_CODE: i32 = -32005;

/// Marker the storage layer embeds in its get_logs overflow error
/// (`rome-evm-client::pg_storage` "query returned more than N results …").
/// String-matched here because the SDK surfaces it as `RomeEvmError::Custom`.
const GET_LOGS_OVERFLOW_MARKER: &str = "returned more than";

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Response failed: {0}")]
    ResponseFailed(ErrorObjectOwned),

    #[error("Rome Program Error {0}")]
    RomeProgramError(RomeProgramError),

    #[error("Rome-EVM SDK error: {0}")]
    RomeEvmError(RomeEvmError),

    #[error("Solana client error: {0}")]
    SolanaClientError(ClientError),
    
    #[error("Custom error: {0}")]
    Custom(String),
}

impl From<ApiError> for ErrorObjectOwned {
    fn from(e: ApiError) -> ErrorObjectOwned {
        match e {
            ApiError::ResponseFailed(e) => e,
            ApiError::RomeEvmError(RomeEvmError::EmulationRevert(mes, hex)) => {
                ErrorObjectOwned::owned(3, mes, Some(hex))
            }
            ApiError::RomeEvmError(RomeEvmError::EmulationError(err)) => {
                ErrorObjectOwned::owned(3, err, None::<String>)
            }
            ApiError::RomeEvmError(RomeEvmError::RomeProgramError(err)) => {
                let data = revert_msg(err.to_string());
                let hex = format!("0x{}", hex::encode(data));
                ErrorObjectOwned::owned(3, err.to_string(), Some(hex))
            }
            ApiError::SolanaClientError(err) => {
                ErrorObjectOwned::owned(INTERNAL_ERROR_CODE, err.to_string(), None::<String>)
            }
            ApiError::RomeProgramError(err) => {
                let data = revert_msg(err.to_string());
                let hex = format!("0x{}", hex::encode(data));
                ErrorObjectOwned::owned(3, err.to_string(), Some(hex))
            }
            ApiError::RomeEvmError(RomeEvmError::Custom(msg))
                if msg.contains("get_logs") && msg.contains(GET_LOGS_OVERFLOW_MARKER) =>
            {
                ErrorObjectOwned::owned(LOGS_LIMIT_EXCEEDED_CODE, msg, None::<String>)
            }
            // Catch-all: surface the error's Display instead of an empty string.
            // Unmatched typed variants (TooManyAccounts, NoFreeHolders,
            // HeapExhausted, …) otherwise reach tooling as a blank JSON-RPC
            // message, which cast/forge/MetaMask render as nothing.
            other => {
                ErrorObjectOwned::owned(CALL_EXECUTION_FAILED_CODE, other.to_string(), None::<String>)
            }
        }
    }
}

impl From<RomeEvmError> for ApiError {
    fn from(value: RomeEvmError) -> Self {
        Self::RomeEvmError(value)
    }
}

impl From<ClientError> for ApiError {
    fn from(value: ClientError) -> Self {
        Self::SolanaClientError(value)
    }
}

impl From<RomeProgramError> for ApiError {
    fn from(value: RomeProgramError) -> Self {
        Self::RomeProgramError(value)
    }
}

impl ApiError {
    pub fn custom(msg: impl Into<String>) -> Self {
        Self::Custom(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    //! Pin the JSON-RPC mapping of the storage layer's get_logs overflow
    //! error: callers (web3.py / ethers / viem) recognize -32005 as the
    //! standard "query returned too many results, paginate" signal that
    //! Infura/Alchemy/geth emit — it must NOT surface as a generic
    //! call-execution failure.
    use super::*;

    #[test]
    fn get_logs_overflow_maps_to_limit_exceeded_code() {
        let storage_err = RomeEvmError::Custom(
            "get_logs query returned more than 50000 results — narrow the block range or filters"
                .to_string(),
        );
        let obj: ErrorObjectOwned = ApiError::RomeEvmError(storage_err).into();
        assert_eq!(obj.code(), LOGS_LIMIT_EXCEEDED_CODE);
        assert!(obj.message().contains("more than 50000"));
    }

    /// Other Custom storage errors keep today's generic mapping.
    #[test]
    fn unrelated_custom_errors_keep_generic_mapping() {
        let obj: ErrorObjectOwned =
            ApiError::RomeEvmError(RomeEvmError::Custom("boom".into())).into();
        assert_eq!(obj.code(), CALL_EXECUTION_FAILED_CODE);
    }

    /// Unmatched typed variants (e.g. `TooManyAccounts`, the recurring
    /// estimateGas failure for airdrop/multisend txs over the 62-account cap)
    /// must NOT surface as a blank JSON-RPC message — cast/forge/MetaMask show
    /// the user nothing. The catch-all arm must emit the variant's Display.
    #[test]
    fn unmatched_variant_surfaces_nonempty_display_message() {
        let obj: ErrorObjectOwned =
            ApiError::RomeEvmError(RomeEvmError::TooManyAccounts(63)).into();
        assert_eq!(obj.code(), CALL_EXECUTION_FAILED_CODE);
        assert!(
            !obj.message().is_empty(),
            "blank error message is useless to tooling"
        );
        // Surface the SDK error's own Display — assert against it rather than a
        // hardcoded cap so an SDK cap change can't silently stale this test.
        let display = RomeEvmError::TooManyAccounts(63).to_string();
        assert!(
            obj.message().contains(&display),
            "should surface the Display (want {display:?}, got: {:?})",
            obj.message()
        );
    }
}
