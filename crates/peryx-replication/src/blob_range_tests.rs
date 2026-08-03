use crate::blob_range::{RangeRequest, parse_range};

#[test]
fn test_parse_range_maps_each_header_to_its_outcome() {
    for (header, total, expected) in [
        (None, 1000_u64, RangeRequest::Whole),
        (Some("items=0-1"), 1000, RangeRequest::Whole),
        (Some("bytes=0-1,3-4"), 1000, RangeRequest::Whole),
        (Some("bytes=5"), 1000, RangeRequest::Whole),
        (Some("bytes=-"), 1000, RangeRequest::Whole),
        (Some("bytes=a-b"), 1000, RangeRequest::Whole),
        (Some("bytes=x-"), 1000, RangeRequest::Whole),
        (Some("bytes=-x"), 1000, RangeRequest::Whole),
        (Some("bytes=0-499"), 1000, RangeRequest::Partial(0..500)),
        (Some("bytes=0-100000"), 500, RangeRequest::Partial(0..500)),
        (Some("bytes=500-"), 1000, RangeRequest::Partial(500..1000)),
        (Some("bytes=-500"), 1000, RangeRequest::Partial(500..1000)),
        (Some("bytes=-5000"), 500, RangeRequest::Partial(0..500)),
        (Some("bytes=-0"), 500, RangeRequest::Unsatisfiable),
        (Some("bytes=5-2"), 1000, RangeRequest::Unsatisfiable),
        (Some("bytes=500-"), 500, RangeRequest::Unsatisfiable),
        (Some("bytes=600-699"), 500, RangeRequest::Unsatisfiable),
        (Some("bytes=500-600"), 500, RangeRequest::Unsatisfiable),
    ] {
        assert_eq!(parse_range(header, total), expected, "{header:?} of {total}");
    }
}
