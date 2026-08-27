use super::shared::{
    OperationBuilder, ParameterIn, api_json_response, bounded_integer_parameter, enum_parameter, json, parameter,
    route_param,
};

pub(super) fn package_search() -> OperationBuilder {
    OperationBuilder::new()
        .tag("search")
        .summary(Some("Search one PyPI index route"))
        .description(Some(
            "Searches PyPI projects derived from cached listings, uploads, and metadata. `q` uses substring matching; \
             prefix it with `re:` for a regex. Policy-denied projects are not indexed.",
        ))
        .parameter(parameter(
            "q",
            ParameterIn::Query,
            "Search text. Prefix with `re:` to use a regex.",
            json!("widget"),
        ))
        .parameter(enum_parameter(
            "type",
            ParameterIn::Query,
            "`uploaded`, `cached`, or `override`; omit for all sources.",
            json!("override"),
            [json!("uploaded"), json!("cached"), json!("override")],
        ))
        .parameter(enum_parameter(
            "availability",
            ParameterIn::Query,
            "`local` returns projects with locally available files; omit or use `all` for every indexed project.",
            json!("local"),
            [json!("local"), json!("all")],
        ))
        .parameter(bounded_integer_parameter(
            "page",
            ParameterIn::Query,
            "One-based page number.",
            json!(1),
            Some(1),
            None,
        ))
        .parameter(enum_parameter(
            "page_size",
            ParameterIn::Query,
            "Page size: 25, 50, or 100.",
            json!(25),
            [json!(25), json!(50), json!(100)],
        ))
        .parameter(route_param())
        .response(
            "200",
            api_json_response(
                "Search results",
                json!({
                    "query": "widget",
                    "type": "all",
                    "availability": "all",
                    "page": 1,
                    "page_size": 25,
                    "total": 1,
                    "results": [{
                        "display_name": "Widget",
                        "normalized_name": "widget",
                        "route": "team/packages",
                        "index": "team/packages",
                        "type": "cached",
                        "available": true,
                        "summary": "A Python package."
                    }]
                }),
            ),
        )
        .response(
            "400",
            api_json_response(
                "Invalid search parameters",
                json!({"error": "invalid package source type"}),
            ),
        )
}
