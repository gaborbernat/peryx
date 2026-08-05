//! Structured, redacted telemetry for one replication operation.
//!
//! An [`OperationEnvelope`] carries who authored an operation, under which authority epoch, at what
//! serial, its kind, and the W3C trace it belongs to. This turns that into the log-safe fields a span or
//! event records: the operation identity, the kind, and the traceparent, and nothing from the change
//! payload. A credential, artifact bytes, or a private path travels in the change, never in these fields,
//! so it cannot reach a log through this surface.
//!
//! Emitting is gated on the trace's own sampled flag, so an operation a producer chose not to trace adds
//! no event volume here either. The apply side joins the author's trace through
//! [`derive_child`](crate::derive_child); this records the correlated identity a joined span carries.

use serde::Serialize;

use crate::envelope::OperationEnvelope;

/// The log-safe fields of one operation: its identity, kind, and trace linkage, and nothing from its
/// payload.
///
/// Serializing this is safe to log. It names the operation by its `(source, epoch, serial)` identity and
/// its kind, and carries the traceparent that correlates it, but never the change bytes, metadata, or
/// blob references the operation mutates, so no credential, artifact byte, or private path is exposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OperationTelemetry {
    pub source: String,
    pub epoch: u64,
    pub serial: u64,
    pub kind: &'static str,
    /// The W3C traceparent that correlates this operation, absent when the producer traced none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Whether the trace opted this operation into recording; an unsampled or untraced operation is not
    /// emitted.
    pub sampled: bool,
}

impl OperationTelemetry {
    /// The log-safe telemetry for `envelope`: its identity, kind, and trace linkage, with the change
    /// payload dropped so nothing it carries reaches a log.
    #[must_use]
    pub fn of(envelope: &OperationEnvelope) -> Self {
        let identity = envelope.identity();
        let traceparent = envelope.trace.as_ref().map(|trace| trace.traceparent.clone());
        Self {
            source: identity.source.to_owned(),
            epoch: identity.epoch.0,
            serial: identity.serial,
            kind: envelope.kind.as_str(),
            sampled: sampled(traceparent.as_deref()),
            traceparent,
        }
    }

    /// Record this operation as a structured tracing event, but only when its trace is sampled, so an
    /// unsampled operation adds no event volume. The fields are the log-safe identity and trace linkage;
    /// the change payload is never among them.
    pub fn emit(&self) {
        // A sampled operation always carries a traceparent, since `of` sets the two together. Gating the
        // extraction on `sampled` keeps an unsampled or untraced operation silent and drops the field
        // without a defaulted placeholder that could never be reached.
        let Some(traceparent) = self.traceparent.as_deref().filter(|_| self.sampled) else {
            return;
        };
        tracing::info!(
            operation.source = %self.source,
            operation.epoch = self.epoch,
            operation.serial = self.serial,
            operation.kind = self.kind,
            operation.traceparent = traceparent,
            "availability operation",
        );
    }
}

/// Whether an operation carrying `traceparent` is sampled.
///
/// The low bit of the traceparent's flags byte is the W3C sampled flag. An operation without a trace
/// context, or one whose flags clear that bit, is not sampled, so it adds no event volume by default.
#[must_use]
pub fn sampled(traceparent: Option<&str>) -> bool {
    traceparent.and_then(trace_flags).is_some_and(|flags| flags & 0x01 != 0)
}

/// The flags byte of a W3C traceparent (`version-traceid-parentid-flags`), or `None` when the trailing
/// field is not a two-hex-digit byte.
fn trace_flags(traceparent: &str) -> Option<u8> {
    let flags = traceparent
        .rfind('-')
        .map_or(traceparent, |dash| &traceparent[dash + 1..]);
    if flags.len() != 2 {
        return None;
    }
    u8::from_str_radix(flags, 16).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{OperationTelemetry, sampled};
    use crate::{
        AuthorityEpoch, BlobReference, Change, MetadataMutation, OperationEnvelope, OperationKind, TraceContext,
    };

    const SAMPLED_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const UNSAMPLED_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";

    /// A change whose payload carries a secret-shaped value, a private path, and blob bytes, none of
    /// which may reach the telemetry.
    fn secret_change() -> Change {
        Change {
            serial: 7,
            event: b"credential=hunter2".to_vec(),
            metadata: vec![MetadataMutation::Put {
                key: "/var/lib/peryx/private/path".to_owned(),
                value: b"secret-digest-map".to_vec(),
            }],
            blobs: vec![BlobReference {
                sha256: "a".repeat(64),
                size: 1024,
            }],
        }
    }

    fn traced(traceparent: Option<&str>) -> OperationEnvelope {
        OperationEnvelope {
            trace: traceparent.map(|traceparent| TraceContext {
                traceparent: traceparent.to_owned(),
                tracestate: None,
            }),
            ..OperationEnvelope::current("primary-a", AuthorityEpoch(3), OperationKind::Upload, secret_change())
        }
    }

    #[test]
    fn test_telemetry_carries_the_identity_kind_and_traceparent() {
        let telemetry = OperationTelemetry::of(&traced(Some(SAMPLED_TRACEPARENT)));

        assert_eq!(telemetry.source, "primary-a");
        assert_eq!(telemetry.epoch, 3);
        assert_eq!(telemetry.serial, 7);
        assert_eq!(telemetry.kind, "upload");
        assert_eq!(telemetry.traceparent.as_deref(), Some(SAMPLED_TRACEPARENT));
        assert!(telemetry.sampled);
    }

    #[test]
    fn test_telemetry_drops_the_change_payload() {
        let rendered = serde_json::to_string(&OperationTelemetry::of(&traced(Some(SAMPLED_TRACEPARENT)))).unwrap();

        for secret in [
            "credential=hunter2",
            "hunter2",
            "secret-digest-map",
            "/var/lib/peryx/private/path",
            &"a".repeat(64),
        ] {
            assert!(
                !rendered.contains(secret),
                "telemetry leaked a payload value: {secret}\n{rendered}"
            );
        }
    }

    #[test]
    fn test_an_untraced_operation_omits_the_traceparent_and_is_not_sampled() {
        let telemetry = OperationTelemetry::of(&traced(None));

        assert_eq!(telemetry.traceparent, None);
        assert!(!telemetry.sampled);
        let rendered = serde_json::to_string(&telemetry).unwrap();
        assert!(
            !rendered.contains("traceparent"),
            "an absent traceparent is omitted: {rendered}"
        );
    }

    #[test]
    fn test_sampled_reads_the_traceparent_flag() {
        assert!(sampled(Some(SAMPLED_TRACEPARENT)));
        assert!(!sampled(Some(UNSAMPLED_TRACEPARENT)));
        assert!(!sampled(None));
    }

    #[test]
    fn test_sampled_rejects_a_malformed_flags_field() {
        assert!(
            !sampled(Some("00-abc-def")),
            "a non-two-digit flags field is not sampled"
        );
        assert!(
            !sampled(Some("00-trace-parent-zz")),
            "a non-hex flags field is not sampled"
        );
        assert!(
            !sampled(Some("nodashes")),
            "a string with no flags field is not sampled"
        );
    }

    /// A `tracing` writer that appends every formatted event to a shared buffer, so a test can read back
    /// exactly what `emit` recorded.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Capture {
        fn recorded(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run `body` under a subscriber that formats its events into a buffer, then return the buffer's
    /// contents. The writer is drained before the read so nothing the subscriber buffered is missed.
    fn captured(body: impl FnOnce()) -> String {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt().with_writer(capture.clone()).finish();
        tracing::subscriber::with_default(subscriber, body);
        std::io::Write::flush(&mut capture.clone()).unwrap();
        capture.recorded()
    }

    #[test]
    fn test_emit_records_a_sampled_operation_without_its_payload() {
        let recorded = captured(|| OperationTelemetry::of(&traced(Some(SAMPLED_TRACEPARENT))).emit());

        assert!(
            recorded.contains("availability operation"),
            "the event is recorded: {recorded}"
        );
        assert!(recorded.contains("primary-a"), "the identity is present: {recorded}");
        assert!(
            recorded.contains(SAMPLED_TRACEPARENT),
            "the traceparent is present: {recorded}"
        );
        for secret in ["hunter2", "secret-digest-map", "/var/lib/peryx/private/path"] {
            assert!(
                !recorded.contains(secret),
                "emit leaked a payload value {secret}: {recorded}"
            );
        }
    }

    #[test]
    fn test_emit_skips_an_unsampled_operation() {
        let recorded = captured(|| OperationTelemetry::of(&traced(Some(UNSAMPLED_TRACEPARENT))).emit());

        assert!(
            recorded.is_empty(),
            "an unsampled operation records nothing: {recorded}"
        );
    }
}
