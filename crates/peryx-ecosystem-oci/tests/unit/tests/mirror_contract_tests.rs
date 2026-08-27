use peryx_driver::serving::{MirrorAction, MirrorDriver, MirrorRequest};
use peryx_index::IndexKind;

use super::{app_with, oci_index};
use crate::registry::OciRegistry;

#[rstest::rstest]
#[case::configured_requirements("requirements", false)]
#[case::configured_mode("mode", false)]
#[case::configured_python_tags("python_tags", false)]
#[case::configured_metadata_only("metadata_only", false)]
#[case::override_requirements("requirements", true)]
#[case::override_mode("mode", true)]
#[case::override_python_tags("python_tags", true)]
#[case::override_metadata_only("metadata_only", true)]
#[case::override_packages("packages", true)]
fn prefetch_options_reject_unsupported_keys(#[case] key: &str, #[case] override_option: bool) {
    let mut configured = toml::Table::new();
    let mut overrides = toml::Table::new();
    if override_option {
        overrides.insert(key.to_owned(), toml::Value::Boolean(true));
    } else {
        configured.insert(key.to_owned(), toml::Value::Boolean(true));
    }

    assert_eq!(
        OciRegistry::default()
            .validate_options(&configured, &overrides)
            .unwrap_err(),
        format!("prefetch option {key:?} is not supported by oci")
    );
}

#[rstest::rstest]
#[case::images(images(&[]), images(&[]))]
#[case::packages(
    toml::Table::from_iter([("packages".to_owned(), toml::Value::Array(Vec::new()))]),
    toml::Table::new()
)]
fn prefetch_options_accept_consumed_keys(#[case] configured: toml::Table, #[case] overrides: toml::Table) {
    assert_eq!(OciRegistry::default().validate_options(&configured, &overrides), Ok(()));
}

fn images(values: &[&str]) -> toml::Table {
    toml::Table::from_iter([(
        "images".to_owned(),
        toml::Value::Array(
            values
                .iter()
                .map(|value| toml::Value::String((*value).to_owned()))
                .collect(),
        ),
    )])
}

#[rstest::rstest]
#[case::array_required(toml::Value::Boolean(true), "images must be an array")]
#[case::string_entries(toml::Value::Array(vec![toml::Value::Integer(1)]), "images entries must be strings")]
#[tokio::test]
async fn mirror_rejects_invalid_image_options(#[case] value: toml::Value, #[case] expected: &str) {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = app_with(&dir, oci_index("store", "oci", IndexKind::Hosted { volatile: false }));
    let configured = toml::Table::from_iter([("images".to_owned(), value)]);
    let empty = toml::Table::new();
    let mut output = Vec::new();

    let error = OciRegistry::default()
        .mirror(
            state,
            MirrorRequest {
                action: MirrorAction::Plan,
                index: "oci",
                settings: &empty,
                configured: &configured,
                overrides: &empty,
            },
            &mut output,
        )
        .await
        .unwrap_err();

    assert_eq!(error, expected);
}

#[tokio::test]
async fn mirror_plan_reports_selected_images() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = app_with(&dir, oci_index("store", "oci", IndexKind::Hosted { volatile: false }));
    let configured = images(&["library/example:latest"]);
    let empty = toml::Table::new();
    let mut output = Vec::new();

    OciRegistry::default()
        .mirror(
            state,
            MirrorRequest {
                action: MirrorAction::Plan,
                index: "oci",
                settings: &empty,
                configured: &configured,
                overrides: &empty,
            },
            &mut output,
        )
        .await
        .unwrap();

    let output = String::from_utf8(output).unwrap();
    assert!(output.contains("manifest\tstore\tlibrary/example:latest"));
    assert!(output.contains("summary\tstore\t\timages\t\t\t1\timages"));
}

#[tokio::test]
async fn mirror_rejects_unknown_indexes_and_empty_selections() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = app_with(&dir, oci_index("store", "oci", IndexKind::Hosted { volatile: false }));
    let empty = toml::Table::new();
    let mut output = Vec::new();
    let request = |index| MirrorRequest {
        action: MirrorAction::Plan,
        index,
        settings: &empty,
        configured: &empty,
        overrides: &empty,
    };

    assert!(
        OciRegistry::default()
            .mirror(state.clone(), request("missing"), &mut output)
            .await
            .is_err()
    );
    assert!(
        OciRegistry::default()
            .mirror(state, request("oci"), &mut output)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mirror_surfaces_output_failures() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = app_with(&dir, oci_index("store", "oci", IndexKind::Hosted { volatile: false }));
    let configured = images(&["library/example:latest"]);
    let empty = toml::Table::new();

    let mut output = std::io::Cursor::new(&mut [] as &mut [u8]);
    let error = OciRegistry::default()
        .mirror(
            state,
            MirrorRequest {
                action: MirrorAction::Plan,
                index: "oci",
                settings: &empty,
                configured: &configured,
                overrides: &empty,
            },
            &mut output,
        )
        .await
        .unwrap_err();

    assert_eq!(error, "failed to write whole buffer");
}
