use std::collections::BTreeMap;

use super::sha256;
use crate::{CoreMetadata, File, Provenance, Yanked};

fn file(filename: &str, authoritative_version: Option<&str>) -> File {
    File {
        filename: filename.to_owned(),
        url: "u".to_owned(),
        hashes: BTreeMap::new(),
        requires_python: None,
        size: None,
        upload_time: None,
        yanked: Yanked::No,
        core_metadata: CoreMetadata::Absent,
        dist_info_metadata: CoreMetadata::Absent,
        gpg_sig: None,
        provenance: Provenance::Absent,
        authoritative_version: authoritative_version.map(str::to_owned),
    }
}

#[rstest::rstest]
#[case::wheel("pkg-1.0-py3-none-any.whl", Some("1.0"))]
#[case::wheel_build("pkg-1.0-1-py3-none-any.whl", Some("1.0"))]
#[case::egg("pkg-1.0-py3.egg", Some("1.0"))]
#[case::sdist("python-dateutil-2.8.2.tar.gz", Some("2.8.2"))]
#[case::zip("pkg-1.0.zip", Some("1.0"))]
#[case::legacy("pkg-legacy.tgz", Some("legacy"))]
#[case::release_suffix("pkg-2-1.0.tar.gz", Some("1.0"))]
#[case::raw_suffix("pkg-1.0-custom.tar.gz", Some("custom"))]
#[case::ambiguous("pkg-1.0-1.tar.gz", None)]
#[case::empty("pkg-.tar.gz", None)]
#[case::unsupported("pkg-1.0.exe", None)]
fn test_file_release_version_uses_conservative_filename_fallback(
    #[case] filename: &str,
    #[case] expected: Option<&str>,
) {
    assert_eq!(file(filename, None).release_version(), expected);
}

#[test]
fn test_file_release_version_prefers_stored_publication_version() {
    let file = file("pkg-1.0-1.tar.gz", Some("1.0-1"));

    assert_eq!(file.release_version(), Some("1.0-1"));
    assert!(file.matches_version("1.0.post1"));
}

#[test]
fn test_file_authoritative_version_stays_internal_to_json() {
    let file = file("pkg-1.0-py3-none-any.whl", Some("1.0"));
    let json = serde_json::to_string(&file).unwrap();
    let parsed: File =
        serde_json::from_str(r#"{"filename":"pkg-1.0-py3-none-any.whl","url":"u","authoritative_version":"2.0"}"#)
            .unwrap();

    assert!(!json.contains("authoritative_version"));
    assert_eq!(parsed.authoritative_version, None);
}

#[test]
fn test_file_roundtrips_an_old_serialized_row_unchanged() {
    let row = serde_json::json!({
        "filename": "pkg-1.0.tar.gz",
        "url": "u",
        "hashes": {},
        "yanked": false,
        "core-metadata": false,
    });
    let file: File = serde_json::from_value(row.clone()).unwrap();

    assert_eq!(serde_json::to_value(file).unwrap(), row);
}

#[test]
fn test_file_metadata_helpers_update_both_spellings() {
    let mut file = File {
        filename: "x-1.whl".to_owned(),
        url: "u".to_owned(),
        hashes: BTreeMap::new(),
        requires_python: None,
        size: None,
        upload_time: None,
        yanked: Yanked::No,
        core_metadata: CoreMetadata::Absent,
        dist_info_metadata: CoreMetadata::Available,
        gpg_sig: None,
        provenance: Provenance::Absent,
        authoritative_version: None,
    };
    assert_eq!(file.metadata(), &CoreMetadata::Available);
    file.set_metadata(CoreMetadata::Hashes(sha256("abc")));
    assert_eq!(
        (&file.core_metadata, &file.dist_info_metadata),
        (
            &CoreMetadata::Hashes(sha256("abc")),
            &CoreMetadata::Hashes(sha256("abc"))
        )
    );
    file.clear_metadata();
    assert_eq!(
        (&file.core_metadata, &file.dist_info_metadata),
        (&CoreMetadata::Absent, &CoreMetadata::Absent)
    );
}

#[test]
fn test_yanked_deserialize_variants() {
    assert_eq!(serde_json::from_str::<Yanked>("false").unwrap(), Yanked::No);
    assert_eq!(serde_json::from_str::<Yanked>("true").unwrap(), Yanked::Yes);
    assert_eq!(
        serde_json::from_str::<Yanked>("\"why\"").unwrap(),
        Yanked::Reason("why".to_owned())
    );
}

#[test]
fn test_yanked_deserialize_rejects_number() {
    assert!(serde_json::from_str::<Yanked>("123").is_err());
}

#[test]
fn test_core_metadata_deserialize_variants() {
    assert_eq!(
        serde_json::from_str::<CoreMetadata>("false").unwrap(),
        CoreMetadata::Absent
    );
    assert_eq!(
        serde_json::from_str::<CoreMetadata>("true").unwrap(),
        CoreMetadata::Available
    );
    let hashes = serde_json::from_str::<CoreMetadata>(r#"{"sha256":"abc"}"#).unwrap();
    assert_eq!(hashes, CoreMetadata::Hashes(sha256("abc")));
}

#[test]
fn test_core_metadata_deserialize_rejects_number() {
    assert!(serde_json::from_str::<CoreMetadata>("123").is_err());
}

#[test]
fn test_provenance_deserialize_variants() {
    assert_eq!(serde_json::from_str::<Provenance>("null").unwrap(), Provenance::None);
    assert_eq!(
        serde_json::from_str::<Provenance>(r#""https://example.test/provenance""#).unwrap(),
        Provenance::Url("https://example.test/provenance".to_owned())
    );
    assert!(serde_json::from_str::<Provenance>("123").is_err());
}

#[rstest::rstest]
#[case("https://example.test/pkg.provenance", true)]
#[case("http://localhost/pkg.provenance", true)]
#[case("http://127.0.0.1/pkg.provenance", true)]
#[case("http://[::1]/pkg.provenance", true)]
#[case("http://example.test/pkg.provenance", false)]
#[case("https://user@example.test/pkg.provenance", false)]
#[case("/pkg.provenance", false)]
fn test_provenance_secure_url(#[case] value: &str, #[case] accepted: bool) {
    let provenance = Provenance::Url(value.to_owned());

    assert_eq!(provenance.secure_url(), accepted.then_some(value));
}

#[test]
fn test_provenance_retain_secure_url_drops_only_an_insecure_url() {
    let mut insecure = Provenance::Url("http://example.test/pkg.provenance".to_owned());
    let mut absent = Provenance::Absent;

    insecure.retain_secure_url();
    absent.retain_secure_url();

    assert_eq!(insecure, Provenance::Absent);
    assert_eq!(absent, Provenance::Absent);
}
