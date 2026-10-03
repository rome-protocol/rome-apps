use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    pub tier: String,
    pub uniqueness: String,
    pub why_rome: String,
    pub categories: Vec<String>,
    pub status: String,
    pub owner: Owner,
    pub chain: Chain,
    #[serde(default)]
    pub solana_programs: Vec<String>,
    pub urls: Urls,
    pub capabilities: Vec<Capability>,
    pub surfaces: Surfaces,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_cache: Option<MetricsCache>,
    pub signature: Signature,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Owner {
    pub team: String,
    pub contact: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chain {
    pub rome_chain_id: u64,
    pub contract_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Urls {
    pub app: String,
    pub docs: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
    pub kind: String,
    pub description: String,
    pub inputs: serde_json::Value,
    pub outputs: serde_json::Value,
    pub abi: String,
    pub abi_hash: String,
    pub cu_estimate: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub example_call: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Surfaces {
    pub mcp_tools: Vec<String>,
    pub manifest_url: String,
    pub rest_base: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsCache {
    pub tx_7d: u64,
    pub unique_callers_7d: u64,
    pub agent_share_7d: f64,
    pub gas_usd_7d: f64,
    pub checked_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub alg: String,
    pub pubkey: String,
    pub value: String,
}
