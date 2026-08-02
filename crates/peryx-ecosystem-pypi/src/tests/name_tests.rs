use std::borrow::Cow;

use crate::{PackageName, is_valid_name, normalize_name, normalize_name_cow, project_of_filename};

#[test]
fn test_normalize_name_cow_borrows_already_normal_and_owns_the_rest() {
    for already in ["flask", "django-rest", "a1b2", "-leading", "trailing-"] {
        assert!(matches!(normalize_name_cow(already), Cow::Borrowed(_)), "{already:?}");
        assert_eq!(normalize_name_cow(already), normalize_name(already));
    }
    for rewritten in ["Flask", "zope.interface", "A__B", "foo--bar"] {
        assert!(matches!(normalize_name_cow(rewritten), Cow::Owned(_)), "{rewritten:?}");
        assert_eq!(normalize_name_cow(rewritten), normalize_name(rewritten));
    }
}

#[test]
fn test_normalize_name_matches_pep503() {
    let cases = [
        ("Flask", "flask"),
        ("Django-REST", "django-rest"),
        ("zope.interface", "zope-interface"),
        ("A__B", "a-b"),
        ("foo.bar_baz", "foo-bar-baz"),
        ("already-normal", "already-normal"),
        ("Mixed._-Seps", "mixed-seps"),
        ("UPPER", "upper"),
        ("_leading", "-leading"),
        ("trailing_", "trailing-"),
    ];
    for (input, expected) in cases {
        assert_eq!(normalize_name(input), expected, "{input:?}");
    }
}

#[test]
fn test_package_name_normalizes_and_displays() {
    let name = PackageName::new("Zope.Interface");
    assert_eq!(name.as_str(), "zope-interface");
    assert_eq!(name.to_string(), "zope-interface");
}

#[test]
fn test_package_name_equal_when_normalized_equal() {
    assert_eq!(PackageName::new("Foo_Bar"), PackageName::new("foo-bar"));
}

#[test]
fn test_project_of_filename_keys_the_whole_hyphenated_sdist_name() {
    let cases = [
        ("python-dateutil-2.8.2.tar.gz", "python-dateutil"),
        ("python-dateutil-2.8.2.zip", "python-dateutil"),
        ("python-dateutil-2.8.2.tar.bz2", "python-dateutil"),
        ("flask-1.0.tar.gz", "flask"),
        ("Flask-1.0.tar.gz", "flask"),
        ("flask-1.0-py3-none-any.whl", "flask"),
        ("python_dateutil-2.8.2-py2.py3-none-any.whl", "python-dateutil"),
        ("standalone", "standalone"),
    ];
    for (filename, expected) in cases {
        assert_eq!(project_of_filename(filename), expected, "{filename}");
    }
}

#[test]
fn test_is_valid_name_matches_pypa_shape() {
    for valid in ["a", "Flask", "zope.interface", "a_b-c.1"] {
        assert!(is_valid_name(valid), "{valid}");
    }
    for invalid in ["", "-a", "a-", ".a", "a.", "bad/name", "snow☃"] {
        assert!(!is_valid_name(invalid), "{invalid}");
    }
}
