use cardo_service::manifest::{Ingester, SchemaValidator, Verifier};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn serve_fixtures() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/manifests");
    let handle = tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await { Ok(p) => p, Err(_) => break };
            let fixtures = fixtures.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = match sock.read(&mut buf).await { Ok(n) => n, Err(_) => return };
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req.lines().next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let file_path = match path.as_str() {
                    "/index.json" => fixtures.join("test-index.json"),
                    p if p.ends_with(".json") => fixtures.join(p.trim_start_matches('/')),
                    _ => fixtures.join("missing.json"),
                };
                let body = std::fs::read(&file_path).unwrap_or_default();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.write_all(&body).await;
            });
        }
    });
    (format!("http://{}", addr), handle)
}

fn validator() -> SchemaValidator {
    let raw = include_str!("fixtures/catalog.schema.json");
    SchemaValidator::from_json_str(raw).unwrap()
}

fn verifier() -> Verifier {
    let keys = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/keys");
    Verifier::from_keys_dir(&keys).unwrap()
}

#[tokio::test]
async fn fetches_and_verifies_manifests_from_index() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/manifests");
    // Build a test-index.json that points at signed-minimal.json.
    // Because signed-minimal.json has id="test-app", the ingester will fetch /test-app.json;
    // we symlink/copy so that path resolves to signed-minimal.json's bytes.
    let index = serde_json::json!({
        "version": 1,
        "generated_at": "2026-04-21T00:00:00Z",
        "apps": [{"id": "test-app"}]
    });
    std::fs::write(fixtures.join("test-index.json"), index.to_string()).unwrap();
    std::fs::copy(fixtures.join("signed-minimal.json"), fixtures.join("test-app.json")).unwrap();

    let (base_url, _handle) = serve_fixtures().await;
    let ing = Ingester::new(base_url, validator(), verifier());
    let results = ing.fetch_all().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].manifest.id, "test-app");
}
