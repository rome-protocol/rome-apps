use anyhow::{anyhow, Result};
use jsonschema::{Draft, JSONSchema};
use serde_json::Value;

pub struct SchemaValidator {
    schema: JSONSchema,
}

impl SchemaValidator {
    pub fn from_json_str(s: &str) -> Result<Self> {
        let schema_value: Value = serde_json::from_str(s)?;
        let schema = JSONSchema::options()
            .with_draft(Draft::Draft202012)
            .compile(&schema_value)
            .map_err(|e| anyhow!("compile schema: {}", e))?;
        Ok(Self { schema })
    }

    pub fn validate(&self, value: &Value) -> Result<()> {
        let result = self.schema.validate(value);
        if let Err(errors) = result {
            let msgs: Vec<String> = errors
                .take(5)
                .map(|e| format!("{} at {}", e, e.instance_path))
                .collect();
            return Err(anyhow!("schema validation failed: {}", msgs.join("; ")));
        }
        Ok(())
    }
}
