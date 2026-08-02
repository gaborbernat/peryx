use crate::meta::ArtifactSource;

use super::store;

#[test]
fn test_count_artifact_placements_reports_the_recorded_rows() {
    let (_dir, store) = store();
    assert_eq!(store.count_artifact_placements().unwrap(), 0);

    store
        .record_artifact_placement("sha256:aa", ArtifactSource::Hosted, true)
        .unwrap();
    store
        .record_artifact_placement("sha256:bb", ArtifactSource::Proxy, false)
        .unwrap();
    assert_eq!(store.count_artifact_placements().unwrap(), 2);

    store
        .record_artifact_placement("sha256:aa", ArtifactSource::Hosted, false)
        .unwrap();
    assert_eq!(store.count_artifact_placements().unwrap(), 2);

    store.delete_artifact_placement("sha256:aa").unwrap();
    assert_eq!(store.count_artifact_placements().unwrap(), 1);
}
