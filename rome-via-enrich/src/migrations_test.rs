//! Guard: sqlx keys migrations by numeric version prefix — two files sharing
//! a prefix poison every deployed DB with "previously applied but has been
//! modified" and crash-loop the service on startup (hit live on hadrian-lt
//! 2026-07-30: 0228_evm_tx_method_id_idx vs 0228_token_holders_holder_idx).
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn migration_version_prefixes_are_unique() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
        let mut seen: HashMap<String, String> = HashMap::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if !name.ends_with(".up.sql") {
                continue;
            }
            let version = name.split('_').next().unwrap().to_string();
            if let Some(prev) = seen.insert(version.clone(), name.clone()) {
                panic!("duplicate migration version {version}: {prev} and {name}");
            }
        }
        assert!(!seen.is_empty(), "no migrations found — wrong dir?");
    }
}
