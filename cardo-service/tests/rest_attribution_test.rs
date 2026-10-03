use cardo_service::rest::attribution::ClientKind;

#[test]
fn parses_agent() {
    assert_eq!(ClientKind::from_header(Some("agent")), ClientKind::Agent);
    assert_eq!(ClientKind::from_header(Some("AGENT")), ClientKind::Agent);
}

#[test]
fn parses_human() {
    assert_eq!(ClientKind::from_header(Some("human")), ClientKind::Human);
}

#[test]
fn parses_unknown_fallback() {
    assert_eq!(ClientKind::from_header(None), ClientKind::Unknown);
    assert_eq!(ClientKind::from_header(Some("bot")), ClientKind::Unknown);
    assert_eq!(ClientKind::from_header(Some("")), ClientKind::Unknown);
}
