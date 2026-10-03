use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use std::convert::Infallible;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientKind {
    Agent,
    Human,
    Unknown,
}

impl ClientKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Human => "human",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_header(v: Option<&str>) -> Self {
        match v.map(str::to_ascii_lowercase).as_deref() {
            Some("agent") => Self::Agent,
            Some("human") => Self::Human,
            _ => Self::Unknown,
        }
    }
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for ClientKind {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get("X-Rome-Client-Kind")
            .and_then(|v| v.to_str().ok());
        Ok(ClientKind::from_header(header))
    }
}
