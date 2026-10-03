use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AppSummary {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tier: String,
    pub uniqueness: String,
    pub status: String,
    pub categories: Vec<String>,
    pub manifest_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AppListResponse {
    pub apps: Vec<AppSummary>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AppListQuery {
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

fn default_limit() -> u32 {
    50
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct QuoteRequest {
    pub from: String,
    pub inputs: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct QuoteResponse {
    pub outputs: serde_json::Value,
    pub gas_used: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cu_used: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExecuteRequest {
    pub from: String,
    pub inputs: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gas_limit: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UnsignedTxResponse {
    pub to: String,
    pub data: String,
    pub value: String,
    pub gas_limit: String,
    pub chain_id: u64,
    pub nonce: u64,
    pub max_fee_per_gas: String,
    pub max_priority_fee_per_gas: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MetricsResponse {
    pub tx_7d: u64,
    pub unique_callers_7d: u64,
    pub agent_share_7d: f64,
    pub gas_usd_7d: f64,
    pub as_of: String,
}
