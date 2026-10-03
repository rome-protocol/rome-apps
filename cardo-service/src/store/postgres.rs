use crate::manifest::Manifest;
use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn upsert_manifest(&self, m: &Manifest, raw: &Value) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO apps (
                id, name, description, icon_url, tier, uniqueness, why_rome,
                categories, status, owner_team, owner_contact,
                rome_chain_id, contract_addr, solana_programs,
                url_app, url_docs, url_source,
                mcp_tools, manifest_url, rest_base,
                tags, signature_pub, signature_val, manifest_raw
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,
                $8,$9,$10,$11,
                $12,$13,$14,
                $15,$16,$17,
                $18,$19,$20,
                $21,$22,$23,$24
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                icon_url = EXCLUDED.icon_url,
                tier = EXCLUDED.tier,
                uniqueness = EXCLUDED.uniqueness,
                why_rome = EXCLUDED.why_rome,
                categories = EXCLUDED.categories,
                status = EXCLUDED.status,
                owner_team = EXCLUDED.owner_team,
                owner_contact = EXCLUDED.owner_contact,
                rome_chain_id = EXCLUDED.rome_chain_id,
                contract_addr = EXCLUDED.contract_addr,
                solana_programs = EXCLUDED.solana_programs,
                url_app = EXCLUDED.url_app,
                url_docs = EXCLUDED.url_docs,
                url_source = EXCLUDED.url_source,
                mcp_tools = EXCLUDED.mcp_tools,
                manifest_url = EXCLUDED.manifest_url,
                rest_base = EXCLUDED.rest_base,
                tags = EXCLUDED.tags,
                signature_pub = EXCLUDED.signature_pub,
                signature_val = EXCLUDED.signature_val,
                manifest_raw = EXCLUDED.manifest_raw,
                ingested_at = NOW()",
        )
        .bind(&m.id)
        .bind(&m.name)
        .bind(&m.description)
        .bind(&m.icon_url)
        .bind(&m.tier)
        .bind(&m.uniqueness)
        .bind(&m.why_rome)
        .bind(&m.categories)
        .bind(&m.status)
        .bind(&m.owner.team)
        .bind(&m.owner.contact)
        .bind(m.chain.rome_chain_id as i64)
        .bind(&m.chain.contract_address)
        .bind(&m.solana_programs)
        .bind(&m.urls.app)
        .bind(&m.urls.docs)
        .bind(&m.urls.source)
        .bind(&m.surfaces.mcp_tools)
        .bind(&m.surfaces.manifest_url)
        .bind(&m.surfaces.rest_base)
        .bind(&m.tags)
        .bind(&m.signature.pubkey)
        .bind(&m.signature.value)
        .bind(raw)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM app_capabilities WHERE app_id = $1")
            .bind(&m.id)
            .execute(&mut *tx)
            .await?;

        for cap in &m.capabilities {
            sqlx::query(
                "INSERT INTO app_capabilities
                    (app_id, name, kind, description, inputs, outputs, abi, abi_hash, cu_estimate, example_call)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            )
            .bind(&m.id)
            .bind(&cap.name)
            .bind(&cap.kind)
            .bind(&cap.description)
            .bind(&cap.inputs)
            .bind(&cap.outputs)
            .bind(&cap.abi)
            .bind(&cap.abi_hash)
            .bind(cap.cu_estimate as i64)
            .bind(&cap.example_call)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_manifest_ids(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM apps ORDER BY id")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}
