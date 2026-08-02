use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rstest::rstest;

use super::toml_config;
use crate::config::{self, AvailabilityConfig, Config, ReplicationConfig, SecretSource};

#[test]
fn test_dc_primary_replication_from_toml() {
    let config = toml_config(
        "[availability]\nmode = \"dc\"\n[availability.replication]\nrole = \"primary\"\nsource = \"primary-a\"\n\
         token_file = \"/run/secrets/replica\"\n",
    );

    assert_eq!(
        config.availability,
        AvailabilityConfig::Dc(ReplicationConfig::Primary {
            source: "primary-a".to_owned(),
            token: SecretSource::File(PathBuf::from("/run/secrets/replica")),
        })
    );
}

#[test]
fn test_ha_replica_replication_from_toml_uses_defaults() {
    let config = toml_config(
        "[availability]\nmode = \"ha\"\n[availability.replication]\nrole = \"replica\"\n\
         upstream = \"https://primary.example/\"\ntoken = \"secret\"\n",
    );

    assert_eq!(
        config.availability,
        AvailabilityConfig::Ha(ReplicationConfig::Replica {
            upstream: "https://primary.example/".to_owned(),
            token: SecretSource::Literal("secret".to_owned()),
            poll_interval: Duration::from_secs(1),
            page_size: NonZeroUsize::new(100).unwrap(),
        })
    );
}

#[test]
fn test_replica_replication_from_toml_accepts_runtime_bounds() {
    let config = toml_config(
        "[availability]\nmode = \"dc\"\n[availability.replication]\nrole = \"replica\"\n\
         upstream = \"https://primary.example/\"\ntoken = \"secret\"\npoll_interval_secs = 30\npage_size = 250\n",
    );

    let AvailabilityConfig::Dc(ReplicationConfig::Replica {
        poll_interval,
        page_size,
        ..
    }) = config.availability
    else {
        panic!("expected a dc replica configuration");
    };
    assert_eq!(poll_interval, Duration::from_secs(30));
    assert_eq!(page_size, NonZeroUsize::new(250).unwrap());
}

#[rstest]
#[case::empty_source("role = \"primary\"\nsource = \"\"\ntoken = \"secret\"", "primary `source`")]
#[case::empty_upstream("role = \"replica\"\nupstream = \"\"\ntoken = \"secret\"", "replica `upstream`")]
#[case::missing_token("role = \"primary\"\nsource = \"primary-a\"", "role needs")]
#[case::empty_token(
    "role = \"primary\"\nsource = \"primary-a\"\ntoken = \"\"",
    "`token` must not be empty"
)]
#[case::duplicate_token(
    "role = \"primary\"\nsource = \"primary-a\"\ntoken = \"secret\"\ntoken_file = \"secret.txt\"",
    "at most one"
)]
#[case::zero_poll(
    "role = \"replica\"\nupstream = \"https://primary.example\"\ntoken = \"secret\"\npoll_interval_secs = 0",
    "`poll_interval_secs` must be positive"
)]
#[case::zero_page(
    "role = \"replica\"\nupstream = \"https://primary.example\"\ntoken = \"secret\"\npage_size = 0",
    "`page_size` must be positive"
)]
#[case::large_page(
    "role = \"replica\"\nupstream = \"https://primary.example\"\ntoken = \"secret\"\npage_size = 1001",
    "exceeds the primary limit"
)]
fn test_replication_rejects_invalid_configuration(#[case] role: &str, #[case] expected: &str) {
    let text = format!("[availability]\nmode = \"dc\"\n[availability.replication]\n{role}\n");
    let partial = config::from_toml(PathBuf::from("x.toml"), &text).unwrap();

    let error = Config::default().apply(partial).unwrap_err();

    assert!(error.to_string().contains(expected), "{error}");
}

#[test]
fn test_legacy_replication_primary_migrates_to_dc() {
    let config = capture_logs(|| {
        toml_config("[replication]\nrole = \"primary\"\nsource = \"writer-a\"\ntoken_file = \"/run/secrets/replica\"\n")
    })
    .0;

    assert_eq!(
        config.availability,
        AvailabilityConfig::Dc(ReplicationConfig::Primary {
            source: "writer-a".to_owned(),
            token: SecretSource::File(PathBuf::from("/run/secrets/replica")),
        })
    );
    assert!(
        config.availability_listener.is_none(),
        "legacy config has no control listener"
    );
}

#[test]
fn test_legacy_replication_replica_migrates_to_dc_with_bounds() {
    let config = capture_logs(|| {
        toml_config(
            "[replication]\nrole = \"replica\"\nupstream = \"https://primary.example/\"\ntoken = \"secret\"\n\
             poll_interval_secs = 30\npage_size = 250\n",
        )
    })
    .0;

    assert_eq!(
        config.availability,
        AvailabilityConfig::Dc(ReplicationConfig::Replica {
            upstream: "https://primary.example/".to_owned(),
            token: SecretSource::Literal("secret".to_owned()),
            poll_interval: Duration::from_secs(30),
            page_size: NonZeroUsize::new(250).unwrap(),
        })
    );
}

#[test]
fn test_legacy_replication_and_availability_together_is_rejected() {
    let text = "[replication]\nrole = \"primary\"\nsource = \"writer-a\"\ntoken = \"secret\"\n\
                [availability]\nmode = \"dc\"\n[availability.replication]\nrole = \"primary\"\n\
                source = \"writer-a\"\ntoken = \"secret\"\n";
    let partial = config::from_toml(PathBuf::from("x.toml"), text).unwrap();

    let error = Config::default().apply(partial).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("not both it and the legacy `[replication]` table"),
        "{error}"
    );
}

#[test]
fn test_legacy_replication_warns_with_migration_details() {
    let logged =
        capture_logs(|| toml_config("[replication]\nrole = \"primary\"\nsource = \"writer-a\"\ntoken = \"secret\"\n"))
            .1;

    assert!(logged.contains("`[replication]`"), "old key: {logged}");
    assert!(logged.contains("`[availability]`"), "replacement: {logged}");
    assert!(logged.contains("dc"), "behavior: {logged}");
    assert!(logged.contains("0.1.0"), "removal version: {logged}");
    assert!(!logged.contains("secret"), "credential leaked: {logged}");
}

/// Run `body` under a `tracing` subscriber that records `WARN` output, returning its result and the
/// captured text so a test can assert both the migrated config and the operator-facing diagnostic.
fn capture_logs<T>(body: impl FnOnce() -> T) -> (T, String) {
    #[derive(Clone)]
    struct Sink(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let sink = Sink(buffer.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    let result = tracing::subscriber::with_default(subscriber, body);
    (result, String::from_utf8(buffer.lock().unwrap().clone()).unwrap())
}
