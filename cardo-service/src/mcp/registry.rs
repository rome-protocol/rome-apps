//! MCP tool registry. Two sets of tools:
//!
//! 1. **Static top-level tools** (5) — `list_apps`, `describe_app`,
//!    `get_metrics`, `quote`, `execute`. Hand-written JSON Schema descriptors
//!    that mirror the REST surface.
//! 2. **Dynamic per-capability tools** (N) — one per row in `app_capabilities`
//!    where `apps.status IN ('live', 'coming-soon')`. Tool name is
//!    `<app_id>.<capability_name>`. The `inputSchema` / `outputSchema` are the
//!    manifest's JSON Schema objects verbatim. Filled by Task 5.
//!
//! `list_tools()` is called on every `tools/list` request — no cache. The
//! query is a single SELECT over a few hundred rows at most, and freshness
//! matters: new capabilities show up on the next `tools/list` without a
//! server-initiated `notifications/tools/list_changed` (v2 feature).

use serde_json::{json, Value};
use sqlx::{PgPool, Row};

/// Central list of tool descriptors — JSON dict per MCP spec `Tool` shape.
pub struct Registry;

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub const fn new() -> Self {
        Self
    }

    /// The 5 top-level static tools. Descriptors hand-written to match the
    /// REST request/response shapes in `src/rest/types.rs`.
    ///
    /// Called synchronously — no DB, no I/O. Also called from Task 3 tests as
    /// a standalone function to avoid needing a full `AppState`.
    pub fn static_tools(&self) -> Vec<Value> {
        vec![
            json!({
                "name": "list_apps",
                "description": "List apps in the Cardo catalog, with optional filters and pagination. Matches REST GET /apps.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "category":   {"type": "string", "description": "Filter: category tag present in `categories`."},
                        "tier":       {"type": "string", "enum": ["featured", "long-tail"], "description": "Filter: tier."},
                        "uniqueness": {"type": "string", "enum": ["only-on-rome", "better-on-rome", "parity"], "description": "Filter: uniqueness classification."},
                        "status":     {"type": "string", "enum": ["live", "coming-soon", "deprecated"], "description": "Filter: status."},
                        "q":          {"type": "string", "description": "Substring match on name or description (ILIKE)."},
                        "limit":      {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
                        "offset":     {"type": "integer", "minimum": 0, "default": 0}
                    },
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "describe_app",
                "description": "Return the full manifest JSON for a single app. Matches REST GET /apps/:id.",
                "inputSchema": {
                    "type": "object",
                    "required": ["id"],
                    "properties": {
                        "id": {"type": "string", "description": "App id (slug)."}
                    },
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "get_metrics",
                "description": "Return cached usage metrics for an app. Matches REST GET /apps/:id/metrics.",
                "inputSchema": {
                    "type": "object",
                    "required": ["app_id"],
                    "properties": {
                        "app_id": {"type": "string", "description": "App id (slug)."}
                    },
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "quote",
                "description": "Simulate a capability call against the rome-evm emulator. Returns return_data + gas_used. Matches REST POST /apps/:id/capabilities/:name/quote.",
                "inputSchema": {
                    "type": "object",
                    "required": ["app_id", "capability", "from", "inputs"],
                    "properties": {
                        "app_id":     {"type": "string"},
                        "capability": {"type": "string"},
                        "from":       {"type": "string", "description": "Caller EVM address (0x…)."},
                        "inputs":     {"type": "object", "description": "Capability inputs; JSON Schema mirrors the manifest capability's `inputs`. v1 requires an empty object."}
                    },
                    "additionalProperties": false
                }
            }),
            json!({
                "name": "execute",
                "description": "Build an unsigned EIP-1559 transaction for a capability call. Caller signs + broadcasts — the server never signs. Matches REST POST /apps/:id/capabilities/:name/execute.",
                "inputSchema": {
                    "type": "object",
                    "required": ["app_id", "capability", "from", "inputs"],
                    "properties": {
                        "app_id":     {"type": "string"},
                        "capability": {"type": "string"},
                        "from":       {"type": "string", "description": "Caller EVM address (0x…)."},
                        "inputs":     {"type": "object", "description": "Capability inputs. v1 requires an empty object."},
                        "gas_limit":  {"type": "string", "description": "Optional decimal gas limit. If absent, the server estimates."}
                    },
                    "additionalProperties": false
                }
            }),
        ]
    }

    /// Enumerate per-capability tools from Postgres. Returns the full tool
    /// list (static + dynamic). On DB error the caller sees the static list
    /// only — the rationale is that MCP clients should still get catalog
    /// navigation if the DB is briefly down, rather than failing the whole
    /// `tools/list`.
    ///
    /// Task 5 implements this fully. For Task 3 we return only the static
    /// list so `tools/list` is exercisable without a live DB.
    pub async fn list_tools(&self, pool: &PgPool) -> Vec<Value> {
        let mut tools = self.static_tools();
        match self.dynamic_tools(pool).await {
            Ok(extra) => tools.extend(extra),
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "dynamic_tools query failed; returning static tools only"
                );
            }
        }
        tools
    }

    /// Query `app_capabilities` JOIN `apps`. Returns one MCP tool descriptor
    /// per capability row (scoped to `apps.status IN ('live', 'coming-soon')`).
    ///
    /// Tool naming convention: `<app_id>.<capability_name>`. Clients get both
    /// idioms at once — the 5 top-level tools (`list_apps`, `quote`, …) for
    /// catalog navigation, plus a per-capability tool per app action. `tools/
    /// list` returns the union.
    ///
    /// Deprecated apps (`status = 'deprecated'`) are hidden from this list so
    /// agents don't call into retired capabilities.
    pub async fn dynamic_tools(&self, pool: &PgPool) -> sqlx::Result<Vec<Value>> {
        let rows = sqlx::query(
            "SELECT c.app_id, c.name, c.kind, c.description, c.inputs, c.outputs \
             FROM app_capabilities c \
             JOIN apps a ON a.id = c.app_id \
             WHERE a.status IN ('live', 'coming-soon') \
             ORDER BY c.app_id, c.name",
        )
        .fetch_all(pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let app_id: String = row.get("app_id");
            let name: String = row.get("name");
            let kind: String = row.get("kind");
            let description: String = row.get("description");
            let inputs: Value = row.get("inputs");
            let outputs: Value = row.get("outputs");
            out.push(json!({
                "name": format!("{app_id}.{name}"),
                "description": format!("[{kind}] {description}"),
                "inputSchema": inputs,
                "outputSchema": outputs,
            }));
        }
        Ok(out)
    }
}
