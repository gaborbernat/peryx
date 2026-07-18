//! Non-secret S3 settings and the credentials resolved apart from them.
//!
//! Endpoint, bucket, prefix, region, addressing, and the request bounds come from configuration.
//! Credentials never do: they resolve from the environment so an access key or secret is not written
//! to a config file, logged, or carried in `Debug` output.

use std::time::Duration;

use reqwest::Url;

/// A configuration value that could not describe a usable S3 backend.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum S3ConfigError {
    #[error("s3 bucket must not be empty")]
    EmptyBucket,
    #[error("s3 region must not be empty")]
    EmptyRegion,
    #[error("s3 endpoint {endpoint:?} is not a valid URL: {reason}")]
    Endpoint { endpoint: String, reason: String },
    #[error("s3 endpoint {endpoint:?} must use http or https")]
    EndpointScheme { endpoint: String },
    #[error("s3 {field} must be greater than zero")]
    Zero { field: &'static str },
}

/// The addressing style an endpoint expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Addressing {
    /// `https://{endpoint}/{bucket}/{key}` — the form S3-compatible servers such as `MinIO` use.
    Path,
    /// `https://{bucket}.{endpoint}/{key}` — AWS virtual-hosted buckets.
    VirtualHost,
}

/// Everything needed to reach a bucket except the credentials.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: Url,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub addressing: S3Addressing,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub upload_concurrency: usize,
}

/// The raw, unvalidated settings a configuration source supplies.
#[derive(Debug, Clone)]
pub struct S3Settings {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub path_style: bool,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub upload_concurrency: usize,
}

impl S3Config {
    /// Validate raw `settings` into a usable configuration.
    ///
    /// # Errors
    /// Returns [`S3ConfigError`] when a required field is empty, the endpoint is not an http(s) URL
    /// with a host, or a bound is zero.
    pub fn new(settings: S3Settings) -> Result<Self, S3ConfigError> {
        if settings.bucket.is_empty() {
            return Err(S3ConfigError::EmptyBucket);
        }
        if settings.region.is_empty() {
            return Err(S3ConfigError::EmptyRegion);
        }
        let endpoint = Url::parse(&settings.endpoint).map_err(|error| S3ConfigError::Endpoint {
            endpoint: settings.endpoint.clone(),
            reason: error.to_string(),
        })?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(S3ConfigError::EndpointScheme {
                endpoint: settings.endpoint,
            });
        }
        for (field, value) in [
            ("request_timeout", settings.request_timeout.as_millis()),
            ("part_size", u128::from(settings.part_size)),
            ("upload_concurrency", settings.upload_concurrency as u128),
        ] {
            if value == 0 {
                return Err(S3ConfigError::Zero { field });
            }
        }
        Ok(Self {
            endpoint,
            bucket: settings.bucket,
            prefix: settings.prefix.trim_matches('/').to_owned(),
            region: settings.region,
            addressing: if settings.path_style {
                S3Addressing::Path
            } else {
                S3Addressing::VirtualHost
            },
            request_timeout: settings.request_timeout,
            max_retries: settings.max_retries,
            multipart_threshold: settings.multipart_threshold,
            part_size: settings.part_size,
            upload_concurrency: settings.upload_concurrency,
        })
    }

    /// The object key a digest maps to, under the configured prefix.
    #[must_use]
    pub fn key_for(&self, digest: &str) -> String {
        if self.prefix.is_empty() {
            format!("sha256/{digest}")
        } else {
            format!("{}/sha256/{digest}", self.prefix)
        }
    }
}

/// S3 credentials. Never sourced from the config file, and its `Debug` hides the secret.
#[derive(Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("session_token", &self.session_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl S3Credentials {
    /// Resolve credentials from the standard AWS environment variables, returning `None` when the
    /// access key or secret is absent.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        Self::from_source(&|key| std::env::var(key).ok())
    }

    // `&dyn` on purpose: a generic source would monomorphize this and its not-empty filters once per
    // caller, and the environment-reading instantiation's filters never run when the AWS variables
    // are unset (as in CI), which the function-coverage gate rejects.
    fn from_source(source: &dyn Fn(&str) -> Option<String>) -> Option<Self> {
        let access_key_id = source("AWS_ACCESS_KEY_ID").filter(|value| !value.is_empty())?;
        let secret_access_key = source("AWS_SECRET_ACCESS_KEY").filter(|value| !value.is_empty())?;
        Some(Self {
            access_key_id,
            secret_access_key,
            session_token: source("AWS_SESSION_TOKEN").filter(|value| !value.is_empty()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{S3Addressing, S3Config, S3ConfigError, S3Credentials, S3Settings};

    fn settings() -> S3Settings {
        S3Settings {
            endpoint: "https://s3.example.com".to_owned(),
            bucket: "bucket".to_owned(),
            prefix: "/cache/".to_owned(),
            region: "us-east-1".to_owned(),
            path_style: true,
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            multipart_threshold: 8 << 20,
            part_size: 8 << 20,
            upload_concurrency: 4,
        }
    }

    #[test]
    fn test_config_trims_prefix_and_maps_addressing() {
        let config = S3Config::new(settings()).unwrap();
        assert_eq!(config.prefix, "cache");
        assert_eq!(config.addressing, S3Addressing::Path);
        assert_eq!(config.key_for("abcd"), "cache/sha256/abcd");
    }

    #[test]
    fn test_config_without_prefix_keys_at_root_and_virtual_host() {
        let config = S3Config::new(S3Settings {
            prefix: String::new(),
            path_style: false,
            ..settings()
        })
        .unwrap();
        assert_eq!(config.key_for("abcd"), "sha256/abcd");
        assert_eq!(config.addressing, S3Addressing::VirtualHost);
    }

    #[test]
    fn test_config_rejects_bad_values() {
        let cases = [
            (
                S3Settings {
                    bucket: String::new(),
                    ..settings()
                },
                S3ConfigError::EmptyBucket,
            ),
            (
                S3Settings {
                    region: String::new(),
                    ..settings()
                },
                S3ConfigError::EmptyRegion,
            ),
            (
                S3Settings {
                    endpoint: "ftp://s3.example.com".to_owned(),
                    ..settings()
                },
                S3ConfigError::EndpointScheme {
                    endpoint: "ftp://s3.example.com".to_owned(),
                },
            ),
            (
                S3Settings {
                    request_timeout: Duration::ZERO,
                    ..settings()
                },
                S3ConfigError::Zero {
                    field: "request_timeout",
                },
            ),
            (
                S3Settings {
                    part_size: 0,
                    ..settings()
                },
                S3ConfigError::Zero { field: "part_size" },
            ),
            (
                S3Settings {
                    upload_concurrency: 0,
                    ..settings()
                },
                S3ConfigError::Zero {
                    field: "upload_concurrency",
                },
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(S3Config::new(input).unwrap_err(), expected);
        }
    }

    #[test]
    fn test_config_rejects_an_unparsable_endpoint() {
        assert!(matches!(
            S3Config::new(S3Settings {
                endpoint: "not a url".to_owned(),
                ..settings()
            })
            .unwrap_err(),
            S3ConfigError::Endpoint { .. }
        ));
    }

    #[test]
    fn test_credentials_resolve_from_source() {
        let full = HashMap::from([
            ("AWS_ACCESS_KEY_ID", "id"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("AWS_SESSION_TOKEN", "token"),
        ]);
        let resolved = S3Credentials::from_source(&|key| full.get(key).map(|value| (*value).to_owned())).unwrap();
        assert_eq!(resolved.access_key_id, "id");
        assert_eq!(resolved.session_token.as_deref(), Some("token"));

        let without_token = S3Credentials::from_source(&|key| match key {
            "AWS_ACCESS_KEY_ID" => Some("id".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_owned()),
            _ => Some(String::new()),
        })
        .unwrap();
        assert!(without_token.session_token.is_none());

        assert!(S3Credentials::from_source(&|key| (key == "AWS_ACCESS_KEY_ID").then(|| "id".to_owned())).is_none());
        assert!(S3Credentials::from_source(&|_| None).is_none());
    }

    #[test]
    fn test_credentials_from_env_agrees_with_the_environment() {
        // Drives `from_env` (and its environment-reading closure) without mutating the environment,
        // which the crate forbids. The presence of credentials must match the AWS variables the
        // process actually carries; the value handling itself is covered through `from_source` above.
        let access = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
        let secret = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
        assert_eq!(
            S3Credentials::from_env().is_some(),
            !access.is_empty() && !secret.is_empty()
        );
    }

    #[test]
    fn test_config_error_display_names_the_problem() {
        assert_eq!(
            S3Config::new(S3Settings {
                bucket: String::new(),
                ..settings()
            })
            .unwrap_err()
            .to_string(),
            "s3 bucket must not be empty"
        );
        assert_eq!(
            S3Config::new(S3Settings {
                region: String::new(),
                ..settings()
            })
            .unwrap_err()
            .to_string(),
            "s3 region must not be empty"
        );
        assert!(
            S3Config::new(S3Settings {
                endpoint: "ftp://s3".to_owned(),
                ..settings()
            })
            .unwrap_err()
            .to_string()
            .contains("must use http or https")
        );
        assert!(
            S3Config::new(S3Settings {
                endpoint: "://bad".to_owned(),
                ..settings()
            })
            .unwrap_err()
            .to_string()
            .contains("is not a valid URL")
        );
        assert_eq!(
            S3Config::new(S3Settings {
                part_size: 0,
                ..settings()
            })
            .unwrap_err()
            .to_string(),
            "s3 part_size must be greater than zero"
        );
    }

    #[test]
    fn test_credentials_debug_redacts_secret() {
        let debug = format!(
            "{:?}",
            S3Credentials {
                access_key_id: "id".to_owned(),
                secret_access_key: "topsecret".to_owned(),
                session_token: Some("sessiontoken".to_owned()),
            }
        );
        assert!(debug.contains("id"));
        assert!(!debug.contains("topsecret"));
        assert!(!debug.contains("sessiontoken"));
        assert!(debug.contains("<redacted>"));
    }
}
