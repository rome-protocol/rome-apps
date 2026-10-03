use crate::manifest::{Manifest, SchemaValidator, Verifier, verify_manifest};
use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

pub struct Ingester {
    client: Client,
    base_url: String,
    validator: SchemaValidator,
    verifier: Verifier,
}

pub struct IngestedManifest {
    pub manifest: Manifest,
    pub raw: Value,
}

impl Ingester {
    pub fn new(base_url: impl Into<String>, validator: SchemaValidator, verifier: Verifier) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        Self { client, base_url: base_url.into(), validator, verifier }
    }

    pub async fn fetch_all(&self) -> Result<Vec<IngestedManifest>> {
        let index_url = format!("{}/index.json", self.base_url.trim_end_matches('/'));
        let index: Value = self.client.get(&index_url).send().await?.json().await?;
        let apps = index.get("apps").and_then(|a| a.as_array())
            .context("index has no apps[]")?;

        let mut results = Vec::new();
        for entry in apps {
            let id = entry.get("id").and_then(|i| i.as_str())
                .context("index entry missing id")?;
            let url = format!("{}/{}.json", self.base_url.trim_end_matches('/'), id);
            let raw: Value = self.client.get(&url).send().await?.json().await?;
            self.validator.validate(&raw).with_context(|| format!("schema validate {}", id))?;
            verify_manifest(&self.verifier, &raw).with_context(|| format!("signature verify {}", id))?;
            let manifest: Manifest = serde_json::from_value(raw.clone())
                .with_context(|| format!("deserialize {}", id))?;
            results.push(IngestedManifest { manifest, raw });
        }
        Ok(results)
    }
}
