use std::path::PathBuf;

use rstest::rstest;

use super::{FailImmediately, config_at};
use crate::app::config_check;
use crate::config::{AcmeConfig, LogSink, TlsConfig};

#[rstest]
#[case::http_plural(
    None,
    false,
    "  listen: http://127.0.0.1:4433\n",
    "  indexes: 6 configured indexes\n"
)]
#[case::https_singular(
    Some(TlsConfig::Manual { cert: PathBuf::from("/cert.pem"), key: PathBuf::from("/key.pem") }),
    true,
    "  listen: https://127.0.0.1:4433\n",
    "  indexes: 1 configured index\n",
)]
#[case::acme(
    Some(TlsConfig::Acme(AcmeConfig {
        domains: vec!["packages.example".to_owned()],
        contact: "ops@example".to_owned(),
        cache_dir: PathBuf::from("/acme"),
        staging: false,
    })),
    false,
    "  listen: https+acme://127.0.0.1:4433\n",
    "  indexes: 6 configured indexes\n",
)]
fn test_config_check_summarizes_the_listener(
    #[case] tls: Option<TlsConfig>,
    #[case] single_index: bool,
    #[case] listen: &str,
    #[case] indexes: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_at(&dir);
    config.tls = tls;
    if single_index {
        config.indexes.truncate(1);
    }
    let mut out = Vec::new();

    config_check(&config, &mut out).unwrap();

    let text = String::from_utf8(out).unwrap();
    assert!(text.contains("configuration is valid\n"), "{text}");
    assert!(text.contains(listen), "{text}");
    assert!(text.contains(indexes), "{text}");
}

#[test]
fn test_config_check_surfaces_a_configuration_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_at(&dir);
    config.log.sink = LogSink::File;

    assert!(config_check(&config, &mut Vec::new()).is_err());
}

#[test]
fn test_config_check_propagates_a_write_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_at(&dir);

    assert!(config_check(&config, &mut FailImmediately).is_err());
}
