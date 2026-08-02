use std::io::Write as _;

use rstest::rstest;

use crate::archive::{Member, MemberKind, list_members};

const BODY: &[u8] = b"body\n";

fn zip_with(path: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        zip.start_file(path, zip::write::SimpleFileOptions::default()).unwrap();
        zip.write_all(BODY).unwrap();
        zip.finish().unwrap();
    }
    buf
}

#[rstest]
#[case::readme("README", MemberKind::Text)]
#[case::license("LICENSE", MemberKind::Text)]
#[case::copying("COPYING", MemberKind::Text)]
#[case::authors("AUTHORS", MemberKind::Text)]
#[case::changelog("CHANGELOG", MemberKind::Text)]
#[case::manifest_in("MANIFEST.in", MemberKind::Text)]
#[case::makefile("Makefile", MemberKind::Text)]
#[case::dockerfile("Dockerfile", MemberKind::Text)]
#[case::containerfile("Containerfile", MemberKind::Text)]
#[case::nested_license("pkg-1.0/LICENSE", MemberKind::Text)]
#[case::extension_form_still_text("README.md", MemberKind::Text)]
#[case::exact_match_not_prefix("NOTICES", MemberKind::Unknown)]
#[case::other_extensionless_stays_unknown("notes", MemberKind::Unknown)]
#[case::binary_extension_stays_binary("logo.png", MemberKind::Binary)]
fn test_conventional_extensionless_names_classify_as_text(#[case] path: &str, #[case] expected: MemberKind) {
    assert_eq!(
        list_members("pkg-1.0.zip", &zip_with(path)).unwrap(),
        vec![Member {
            path: path.to_owned(),
            size: BODY.len() as u64,
            kind: expected,
            previewable: expected == MemberKind::Text,
        }],
    );
}
