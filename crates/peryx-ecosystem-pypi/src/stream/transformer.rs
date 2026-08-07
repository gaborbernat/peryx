//! The chunk-at-a-time lexer that rewrites one upstream PEP 691 page as it streams.

use std::collections::BTreeSet;

use peryx_core::path::{is_local_file_url, local_file_url};
use peryx_policy::PolicyAction;
use peryx_policy::RemoteMetadataMode;
use serde::Serialize;

use super::validator::JsonValidator;
use super::{PageContext, PageSummary, Registration, TransformError};
use crate::policy::PypiPolicy;
use crate::simple::absolutize;
use crate::{CoreMetadata, File, parse_meta};

/// The most raw bytes peryx will read from one upstream Simple page before refusing it. The biggest
/// real project pages (botocore and friends) sit in the low single-digit MiB; a page an order of
/// magnitude past that is pathological, and parsing it unbounded is the memory-exhaustion vector this
/// guards against.
pub const MAX_PAGE_BYTES: usize = 64 * 1024 * 1024;
/// The most file entries peryx will transform from one upstream Simple page. Bytes alone do not bound
/// per-element work: a page of many tiny file objects stays small yet still forces a parse, a policy
/// check, and a registration each, so the element count is capped on its own.
const MAX_PAGE_FILES: usize = 500_000;
const MAX_KEY_BYTES: usize = b"versions".len();

/// The transformer's lexer state, kept across chunk boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Copying bytes through, watching top-level keys.
    Passthrough,
    /// Capturing the top-level `meta` object so peryx can advertise its supported version.
    Meta,
    /// Between `files[` and its matching `]`: elements are captured and rewritten one by one.
    Files,
    /// Between `versions[` and its matching `]`: the whole (small) array is buffered and merged.
    Versions,
}

/// Escape-decoding sub-state for the object key being captured. PEP 691 member names carry meaning
/// (`files`, `meta`, `name`, `versions`), and RFC 8259 lets any character be spelled with an escape,
/// so the key is decoded to its actual value before dispatch: `"files"` matches `files`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyDecode {
    /// Reading literal key bytes.
    Literal,
    /// Saw a backslash; the next byte selects the escape.
    Escape,
    /// Inside a `\uXXXX` sequence, holding the digits seen and the value so far.
    Unicode { seen: u8, value: u16 },
}

/// A chunk-at-a-time rewriter for one upstream page.
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent lexer flags, not a state machine"
)]
pub struct PageTransformer {
    context: PageContext,
    mode: Mode,
    /// Nesting depth relative to the document root.
    depth: u32,
    in_string: bool,
    escaped: bool,
    /// Unknown keys pass through, so storage stops at the longest key that changes output.
    key: [u8; MAX_KEY_BYTES],
    key_len: usize,
    key_decode: KeyDecode,
    capturing_key: bool,
    /// Set between a top-level `"name"` key's colon and its value, so the value is captured.
    expect_name_value: bool,
    capturing_name: bool,
    /// The page's top-level `name`, captured in flight so persistence needs no re-parse.
    name: Vec<u8>,
    /// The top-level `meta` object has been checked.
    meta_seen: bool,
    project_status: Option<String>,
    project_status_reason: Option<String>,
    /// The `files` array opened before `meta` was seen, so a streaming pass cannot know whether the
    /// project is quarantined and its files must be withheld.
    files_before_meta: bool,
    /// The document root has closed; anything but whitespace afterwards is malformed.
    closed: bool,
    trailing: bool,
    /// Element bytes being captured (a `files` object or the whole `versions` array).
    capture: Vec<u8>,
    /// Depth at which the active array closes.
    array_depth: u32,
    emitted_in_array: bool,
    registrations: Vec<Registration>,
    /// Raw upstream bytes fed so far, checked against [`MAX_PAGE_BYTES`].
    consumed: usize,
    /// Upstream file elements captured so far, checked against [`MAX_PAGE_FILES`].
    files_seen: usize,
    /// The full-grammar guard: the structural lexer copies unrecognized bytes through untouched, so
    /// this independently enforces RFC 8259 and the PEP 691 object root over every raw byte.
    validator: JsonValidator,
}

impl PageTransformer {
    #[must_use]
    pub const fn new(context: PageContext) -> Self {
        Self {
            context,
            mode: Mode::Passthrough,
            depth: 0,
            in_string: false,
            escaped: false,
            key: [0; MAX_KEY_BYTES],
            key_len: 0,
            key_decode: KeyDecode::Literal,
            capturing_key: false,
            expect_name_value: false,
            capturing_name: false,
            name: Vec::new(),
            meta_seen: false,
            files_before_meta: false,
            closed: false,
            trailing: false,
            capture: Vec::new(),
            array_depth: 0,
            emitted_in_array: false,
            registrations: Vec::new(),
            consumed: 0,
            files_seen: 0,
            project_status: None,
            project_status_reason: None,
            validator: JsonValidator::new(),
        }
    }

    /// Whether a bounded preflight has enough information to leave the streaming loop: either `meta`
    /// was seen (status known, safe to stream) or `files` opened first (status not yet known, so the
    /// caller buffers the whole page before emitting any file).
    #[must_use]
    pub const fn meta_preflight_done(&self) -> bool {
        self.meta_seen || self.files_before_meta
    }

    /// Whether streaming reached the `files` array before `meta`, which leaves the project status
    /// unknown; the caller then buffers the whole page and transforms it with the status seeded, so
    /// a quarantined project withholds its files regardless of key order.
    #[must_use]
    pub const fn files_precede_meta(&self) -> bool {
        self.files_before_meta
    }

    /// Whether the streaming preflight resolved the project status. Only `meta` carries the status,
    /// so until it is parsed the caller cannot know whether a project is quarantined and must not
    /// emit any file: a preflight that hit its byte cap before either `meta` or `files` leaves the
    /// status unknown just as `files`-before-`meta` does, and both take the buffer-whole-page path.
    #[must_use]
    pub const fn project_status_known(&self) -> bool {
        self.meta_seen
    }

    /// Seed the project status before a whole-page pass so a quarantined page withholds its files
    /// even when `files` precedes `meta` in the document.
    pub fn seed_project_status(&mut self, status: Option<String>) {
        self.project_status = status;
    }

    /// Transform one chunk of upstream bytes, returning the bytes to send downstream.
    ///
    /// # Errors
    /// Returns [`TransformError::Parse`] when a captured element is not valid JSON, or
    /// [`TransformError::TooLarge`] once the page passes [`MAX_PAGE_BYTES`].
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<u8>, TransformError> {
        let mut out = Vec::with_capacity(chunk.len() + 64);
        self.push_into(chunk, &mut out)?;
        Ok(out)
    }

    /// Transform one chunk and append the downstream bytes to an existing buffer.
    ///
    /// Callers that collect chunks can reuse the destination allocation. Existing bytes stay at the
    /// front of `out`.
    ///
    /// # Errors
    /// Returns [`TransformError::Parse`] when a captured element is not valid JSON, or
    /// [`TransformError::TooLarge`] once the page passes [`MAX_PAGE_BYTES`].
    pub fn push_into(&mut self, chunk: &[u8], out: &mut Vec<u8>) -> Result<(), TransformError> {
        self.consumed = self.consumed.saturating_add(chunk.len());
        if self.consumed > MAX_PAGE_BYTES {
            return Err(TransformError::TooLarge);
        }
        out.reserve(chunk.len());
        for &byte in chunk {
            self.validator.feed(byte);
            self.step(byte, out)?;
        }
        Ok(())
    }

    /// Finish the stream, validating that the document closed cleanly.
    ///
    /// # Errors
    /// Returns [`TransformError::Truncated`] when the document ended inside a token,
    /// [`TransformError::Trailing`] when bytes followed the document root, or
    /// [`TransformError::Malformed`] when the bytes were not a single well-formed JSON object.
    pub fn finish(self) -> Result<PageSummary, TransformError> {
        if self.depth != 0 || self.in_string || self.mode != Mode::Passthrough {
            return Err(TransformError::Truncated);
        }
        if self.trailing {
            return Err(TransformError::Trailing);
        }
        self.validator.result()?;
        Ok(PageSummary {
            registrations: self.registrations,
            name: String::from_utf8(self.name).ok().filter(|name| !name.is_empty()),
            project_status: self.project_status,
            project_status_reason: self.project_status_reason,
        })
    }

    fn step(&mut self, byte: u8, out: &mut Vec<u8>) -> Result<(), TransformError> {
        match self.mode {
            Mode::Passthrough => {
                self.step_passthrough(byte, out);
                Ok(())
            }
            Mode::Meta => self.step_meta(byte, out),
            Mode::Files => self.step_files(byte, out),
            Mode::Versions => self.step_versions(byte, out),
        }
    }

    fn step_passthrough(&mut self, byte: u8, out: &mut Vec<u8>) {
        if self.in_string {
            out.push(byte);
            if self.capturing_key && (self.escaped || byte != b'"') {
                self.decode_key_byte(byte);
            }
            if self.capturing_name {
                self.name.push(byte);
            }
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.in_string = false;
                if self.capturing_name {
                    self.name.pop();
                }
                self.capturing_key = false;
                self.capturing_name = false;
            }
            return;
        }
        // Anything but whitespace once the root has closed is trailing garbage, whatever its kind.
        if self.closed && !byte.is_ascii_whitespace() {
            self.trailing = true;
        }
        match byte {
            b'"' => {
                self.in_string = true;
                // A string opening at depth 1 is an object key, or the value of the key just seen.
                if self.depth == 1 {
                    if self.expect_name_value {
                        self.capturing_name = true;
                        self.name.clear();
                    } else {
                        self.key_len = 0;
                        self.key_decode = KeyDecode::Literal;
                        self.capturing_key = true;
                    }
                }
                self.expect_name_value = false;
                out.push(byte);
            }
            b'{' | b'[' => {
                // A non-string `name` value (object, array, ...) still closes the name slot.
                self.expect_name_value = false;
                self.depth += 1;
                // `"files": [` or `"versions": [` at the top level switches modes; the bracket is
                // emitted (files) or captured (versions merges into one emission).
                if byte == b'{' && self.depth == 2 && self.key() == b"meta" {
                    self.mode = Mode::Meta;
                    self.array_depth = self.depth;
                    self.capture.clear();
                    self.capture.push(byte);
                    return;
                }
                if byte == b'[' && self.depth == 2 {
                    if self.key() == b"files" {
                        out.push(byte);
                        self.mode = Mode::Files;
                        self.array_depth = self.depth;
                        self.emitted_in_array = false;
                        self.emit_local_files(out);
                        return;
                    }
                    if self.key() == b"versions" {
                        self.mode = Mode::Versions;
                        self.array_depth = self.depth;
                        self.capture.clear();
                        self.capture.push(byte);
                        return;
                    }
                }
                out.push(byte);
            }
            b'}' | b']' => {
                self.depth = self.depth.saturating_sub(1);
                if self.depth == 0 {
                    self.closed = true;
                }
                out.push(byte);
            }
            b':' if self.depth == 1 => {
                self.expect_name_value = self.key() == b"name";
                if !self.meta_seen && self.key() == b"files" {
                    self.files_before_meta = true;
                }
                out.push(byte);
            }
            _ => {
                // A non-string, non-container `name` value (null, number, ...) closes the name slot.
                if !byte.is_ascii_whitespace() {
                    self.expect_name_value = false;
                }
                out.push(byte);
            }
        }
    }

    /// Feed one raw key byte through the JSON string-escape grammar, appending decoded bytes to the
    /// key buffer so dispatch matches the member name's value, not its spelling.
    fn decode_key_byte(&mut self, byte: u8) {
        match self.key_decode {
            KeyDecode::Literal => {
                if byte == b'\\' {
                    self.key_decode = KeyDecode::Escape;
                } else {
                    self.push_key_byte(byte);
                }
            }
            KeyDecode::Escape => {
                if byte == b'u' {
                    self.key_decode = KeyDecode::Unicode { seen: 0, value: 0 };
                } else {
                    // Only `\uXXXX` can spell an ASCII letter, so only it can build a dispatched
                    // member name. Every other escape resolves to a byte (`"`, `\`, `/`, control) no
                    // control key contains, so a sentinel that matches nothing stands in and the key
                    // falls through to passthrough.
                    self.key_decode = KeyDecode::Literal;
                    self.push_key_byte(0xFF);
                }
            }
            KeyDecode::Unicode { seen, value } => {
                let digit = match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    b'A'..=b'F' => byte - b'A' + 10,
                    _ => {
                        self.key_decode = KeyDecode::Literal;
                        self.push_key_byte(0xFF);
                        return;
                    }
                };
                let value = value << 4 | u16::from(digit);
                if seen + 1 == 4 {
                    self.key_decode = KeyDecode::Literal;
                    self.push_key_codepoint(value);
                } else {
                    self.key_decode = KeyDecode::Unicode { seen: seen + 1, value };
                }
            }
        }
    }

    /// Append a decoded `\uXXXX` codepoint to the key buffer as UTF-8. A lone surrogate is no valid
    /// scalar and no member name peryx dispatches on, so a sentinel byte stands in: it cannot match.
    fn push_key_codepoint(&mut self, codepoint: u16) {
        match char::from_u32(u32::from(codepoint)) {
            Some(decoded) => {
                let mut buffer = [0; 4];
                for &byte in decoded.encode_utf8(&mut buffer).as_bytes() {
                    self.push_key_byte(byte);
                }
            }
            None => self.push_key_byte(0xFF),
        }
    }

    const fn push_key_byte(&mut self, byte: u8) {
        if self.key_len < MAX_KEY_BYTES {
            self.key[self.key_len] = byte;
        }
        self.key_len += 1;
    }

    fn key(&self) -> &[u8] {
        self.key.get(..self.key_len).unwrap_or_default()
    }

    fn step_meta(&mut self, byte: u8, out: &mut Vec<u8>) -> Result<(), TransformError> {
        if self.in_string {
            self.capture.push(byte);
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return Ok(());
        }
        match byte {
            b'"' => {
                self.in_string = true;
                self.capture.push(byte);
            }
            b'{' | b'[' => {
                self.depth += 1;
                self.capture.push(byte);
            }
            b'}' => {
                self.depth = self.depth.saturating_sub(1);
                self.capture.push(byte);
                if self.depth == self.array_depth - 1 {
                    self.emit_meta(out)?;
                    self.capture.clear();
                    self.mode = Mode::Passthrough;
                }
            }
            b']' => {
                self.depth = self.depth.saturating_sub(1);
                self.capture.push(byte);
            }
            _ => self.capture.push(byte),
        }
        Ok(())
    }

    fn step_files(&mut self, byte: u8, out: &mut Vec<u8>) -> Result<(), TransformError> {
        if self.in_string {
            self.capture.push(byte);
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return Ok(());
        }
        match byte {
            b'"' => {
                self.in_string = true;
                self.capture.push(byte);
            }
            b'{' | b'[' => {
                self.depth += 1;
                self.capture.push(byte);
            }
            b'}' => {
                self.depth = self.depth.saturating_sub(1);
                self.capture.push(byte);
                if self.depth == self.array_depth {
                    self.emit_file(out)?;
                    self.capture.clear();
                }
            }
            b']' => {
                self.depth = self.depth.saturating_sub(1);
                if self.depth == self.array_depth - 1 {
                    out.push(b']');
                    self.mode = Mode::Passthrough;
                } else {
                    self.capture.push(byte);
                }
            }
            // Element separators and whitespace between elements: commas are re-managed on emit.
            b',' if self.depth == self.array_depth => {}
            _ if self.capture.is_empty() && byte.is_ascii_whitespace() => {}
            _ => self.capture.push(byte),
        }
        Ok(())
    }

    fn step_versions(&mut self, byte: u8, out: &mut Vec<u8>) -> Result<(), TransformError> {
        if self.in_string {
            self.capture.push(byte);
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return Ok(());
        }
        match byte {
            b'"' => {
                self.in_string = true;
                self.capture.push(byte);
            }
            b'[' | b'{' => {
                self.depth += 1;
                self.capture.push(byte);
            }
            b']' | b'}' => {
                self.depth = self.depth.saturating_sub(1);
                self.capture.push(byte);
                if byte == b']' && self.depth == self.array_depth - 1 {
                    self.emit_versions(out)?;
                    self.capture.clear();
                    self.mode = Mode::Passthrough;
                }
            }
            _ => self.capture.push(byte),
        }
        Ok(())
    }

    /// Locally uploaded files open the array, ahead of anything upstream.
    fn emit_local_files(&mut self, out: &mut Vec<u8>) {
        if self.project_is_quarantined() {
            return;
        }
        for file in &self.context.local_files {
            // Overrides recorded against a filename that was later uploaded locally apply to the
            // local file too, matching the buffered path; the local file otherwise seeds `skip` only
            // to shadow its upstream duplicate, so `hidden`/`yanked` must be consulted here directly.
            if self.context.hidden.contains(&file.filename) {
                continue;
            }
            if self
                .context
                .policy
                .check_file(PolicyAction::Serve, &self.context.project, file)
                .is_err()
            {
                continue;
            }
            if self.emitted_in_array {
                out.push(b',');
            }
            if let Some(yanked) = self.context.yanked.get(&file.filename) {
                let mut file = file.clone();
                file.yanked = yanked.clone();
                write_json(out, &file);
            } else {
                write_json(out, file);
            }
            self.emitted_in_array = true;
        }
    }

    /// Rewrite one captured upstream file object and emit it, unless it is shadowed or hidden.
    fn emit_file(&mut self, out: &mut Vec<u8>) -> Result<(), TransformError> {
        self.files_seen += 1;
        if self.files_seen > MAX_PAGE_FILES {
            return Err(TransformError::TooLarge);
        }
        let mut file: File = serde_json::from_slice(&self.capture)?;
        file.provenance.retain_secure_url();
        if self.project_is_quarantined() {
            return Ok(());
        }
        if self.context.skip.contains(&file.filename) {
            return Ok(());
        }
        if self
            .context
            .policy
            .check_file(PolicyAction::Serve, &self.context.project, &file)
            .is_err()
        {
            return Ok(());
        }
        if let Some(yanked) = self.context.yanked.get(&file.filename) {
            file.yanked = yanked.clone();
        }
        if is_local_file_url(&self.context.route, &file.url) {
            // A legacy cached record already carries peryx-route URLs; serve it as-is, but still drop
            // the gpg-sig since peryx never serves the detached `.asc` at that route.
            file.gpg_sig = None;
            if self.emitted_in_array {
                out.push(b',');
            }
            write_json(out, &file);
            self.emitted_in_array = true;
            return Ok(());
        }
        if let Some(base) = &self.context.base {
            absolutize(base, &mut file.url);
        }
        if let Some(sha256) = file.hashes.get("sha256").cloned() {
            let metadata = if supports_metadata_sibling(&file.filename) {
                match file.metadata() {
                    CoreMetadata::Hashes(hashes) => hashes
                        .get("sha256")
                        .map(|digest| (metadata_sibling(&file.url), digest.clone())),
                    CoreMetadata::Absent | CoreMetadata::Available => None,
                }
            } else {
                None
            };
            if metadata.is_none() {
                file.clear_metadata();
            }
            self.registrations.push(Registration {
                filename: file.filename.clone(),
                sha256: sha256.clone(),
                url: file.url.clone(),
                size: file.size,
                metadata,
                provenance: file.provenance.secure_url().map(str::to_owned),
            });
            if file.metadata().is_absent()
                && let Some(metadata) = self.context.known_metadata.get(&sha256)
            {
                file.set_metadata(CoreMetadata::Hashes(std::collections::BTreeMap::from([(
                    "sha256".to_owned(),
                    metadata.clone(),
                )])));
            }
            file.url = local_file_url(&self.context.route, &sha256, &file.filename);
            if self.context.policy.remote_metadata_mode() != RemoteMetadataMode::Direct
                && file.provenance.secure_url().is_some()
            {
                file.provenance = crate::Provenance::Url(local_file_url(
                    &self.context.route,
                    &sha256,
                    &format!("{}.provenance", file.filename),
                ));
            }
            // The URL now points at peryx's route, which never serves the detached `.asc` sibling,
            // so drop any inherited gpg-sig rather than advertise a signature peryx cannot serve.
            file.gpg_sig = None;
        } else {
            file.clear_metadata();
        }
        if self.emitted_in_array {
            out.push(b',');
        }
        write_json(out, &file);
        self.emitted_in_array = true;
        Ok(())
    }

    /// Rewrite the upstream meta object to peryx's advertised API version.
    fn emit_meta(&mut self, out: &mut Vec<u8>) -> Result<(), TransformError> {
        let meta = parse_meta(&self.capture)?;
        write_json(out, &meta);
        self.project_status = meta.project_status;
        self.project_status_reason = meta.project_status_reason;
        self.meta_seen = true;
        Ok(())
    }

    /// Merge the buffered upstream version array with the local versions and emit it sorted.
    fn emit_versions(&self, out: &mut Vec<u8>) -> Result<(), TransformError> {
        let upstream: Vec<String> = serde_json::from_slice(&self.capture)?;
        let merged: BTreeSet<&str> = upstream
            .iter()
            .chain(&self.context.local_versions)
            .map(String::as_str)
            .collect();
        write_json(out, &merged);
        Ok(())
    }

    fn project_is_quarantined(&self) -> bool {
        self.project_status.as_deref() == Some("quarantined")
    }
}

fn write_json(out: &mut Vec<u8>, value: &impl Serialize) {
    serde_json::to_writer(out, value).expect("simple-API model always serializes to JSON");
}

/// The PEP 658 metadata sibling of a file URL: `.metadata` appended to the path, ahead of any query
/// or fragment. A signed upstream URL like `pkg.whl?token=abc` must yield `pkg.whl.metadata?token=abc`,
/// not `pkg.whl?token=abc.metadata`.
pub fn metadata_sibling(url: &str) -> String {
    let cut = url.find(['?', '#']).unwrap_or(url.len());
    format!("{}.metadata{}", &url[..cut], &url[cut..])
}

fn supports_metadata_sibling(filename: &str) -> bool {
    std::path::Path::new(filename)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("whl"))
        || filename
            .get(filename.len().saturating_sub(7)..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".tar.gz"))
}
