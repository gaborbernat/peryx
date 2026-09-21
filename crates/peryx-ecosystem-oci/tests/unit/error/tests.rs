use super::{ErrorCode, error_response, gateway_timeout};
use axum::http::StatusCode;

#[test]
fn test_every_code_pairs_its_wire_string_with_its_canonical_status() {
    let table = [
        (ErrorCode::BlobUnknown, "BLOB_UNKNOWN", StatusCode::NOT_FOUND),
        (
            ErrorCode::BlobUploadInvalid,
            "BLOB_UPLOAD_INVALID",
            StatusCode::BAD_REQUEST,
        ),
        (
            ErrorCode::BlobUploadUnknown,
            "BLOB_UPLOAD_UNKNOWN",
            StatusCode::NOT_FOUND,
        ),
        (ErrorCode::DigestInvalid, "DIGEST_INVALID", StatusCode::BAD_REQUEST),
        (
            ErrorCode::ManifestBlobUnknown,
            "MANIFEST_BLOB_UNKNOWN",
            StatusCode::BAD_REQUEST,
        ),
        (ErrorCode::ManifestInvalid, "MANIFEST_INVALID", StatusCode::BAD_REQUEST),
        (ErrorCode::ManifestUnknown, "MANIFEST_UNKNOWN", StatusCode::NOT_FOUND),
        (ErrorCode::NameInvalid, "NAME_INVALID", StatusCode::BAD_REQUEST),
        (ErrorCode::NameUnknown, "NAME_UNKNOWN", StatusCode::NOT_FOUND),
        (ErrorCode::SizeInvalid, "SIZE_INVALID", StatusCode::BAD_REQUEST),
        (ErrorCode::Unauthorized, "UNAUTHORIZED", StatusCode::UNAUTHORIZED),
        (ErrorCode::Denied, "DENIED", StatusCode::FORBIDDEN),
        (ErrorCode::Unsupported, "UNSUPPORTED", StatusCode::METHOD_NOT_ALLOWED),
        (
            ErrorCode::TooManyRequests,
            "TOOMANYREQUESTS",
            StatusCode::TOO_MANY_REQUESTS,
        ),
        (ErrorCode::Unavailable, "UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE),
    ];
    for (code, wire, status) in table {
        assert_eq!(code.as_str(), wire);
        assert_eq!(code.status(), status);
        assert_eq!(error_response(code, "x").status(), status);
    }
}

#[tokio::test]
async fn test_gateway_timeout_has_the_fixed_distribution_error() {
    let response = gateway_timeout();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        r#"{"errors":[{"code":"UNKNOWN","message":"upstream request timed out"}]}"#
    );
}
