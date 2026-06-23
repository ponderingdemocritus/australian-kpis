//! OpenAPI document assembly.

use utoipa::OpenApi;

use crate::{
    dataflows::{
        __path_get_dataflow, __path_get_dataflow_codelist, __path_list_dataflows,
        DataflowCodelistResponse, DataflowDetailResponse, DataflowsQuery, DataflowsResponse,
    },
    error::ProblemDetails,
    observations::{
        __path_list_observations, ObservationsMetadata, ObservationsResponse, ObservationsRow,
        PaginationMetadata,
    },
    routes::{__path_health, __path_openapi, HealthResponse},
    scorecards::{__path_aps_config, __path_aps_history, __path_aps_latest, ScorecardHistoryQuery},
    search::{__path_search_catalog, SearchQuery, SearchResponse, SearchResult, SearchResultKind},
    series::{__path_get_series, SeriesLookupResponse, SeriesRevisionMetadata},
    subscriptions::{
        __path_create_subscription, CreateSubscriptionRequest, CreateSubscriptionResponse,
        SubscriptionDetails,
    },
};

/// Root OpenAPI document for the API handlers in this crate.
#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Australian KPIs API",
        version = "0.1.0",
        description = "Unified API for Australian public economic data."
    ),
    paths(
        health,
        openapi,
        list_dataflows,
        get_dataflow,
        get_dataflow_codelist,
        list_observations,
        aps_config,
        aps_latest,
        aps_history,
        get_series,
        search_catalog,
        create_subscription
    ),
    components(schemas(
        au_kpis_domain::Code,
        au_kpis_domain::Codelist,
        au_kpis_domain::Dataflow,
        au_kpis_domain::Dimension,
        au_kpis_domain::Observation,
        au_kpis_domain::Series,
        DataflowCodelistResponse,
        DataflowDetailResponse,
        DataflowsQuery,
        DataflowsResponse,
        HealthResponse,
        ObservationsMetadata,
        ObservationsResponse,
        ObservationsRow,
        PaginationMetadata,
        ProblemDetails,
        au_kpis_scorecard::Axis,
        au_kpis_scorecard::ComponentScore,
        au_kpis_scorecard::Confidence,
        au_kpis_scorecard::ConfidenceBand,
        au_kpis_scorecard::CoverageStatus,
        au_kpis_scorecard::Direction,
        au_kpis_scorecard::IndicatorConfig,
        au_kpis_scorecard::IndicatorContribution,
        au_kpis_scorecard::Normalization,
        au_kpis_scorecard::Provenance,
        au_kpis_scorecard::ScoreZone,
        au_kpis_scorecard::ScorecardConfig,
        au_kpis_scorecard::ScorecardSnapshot,
        au_kpis_scorecard::SubIndexScore,
        au_kpis_scorecard::Trend,
        ScorecardHistoryQuery,
        SearchQuery,
        SearchResponse,
        SearchResult,
        SearchResultKind,
        SeriesLookupResponse,
        SeriesRevisionMetadata,
        CreateSubscriptionRequest,
        CreateSubscriptionResponse,
        SubscriptionDetails
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "dataflows", description = "Dataflow catalog and codelists"),
        (name = "observations", description = "Time-series observations"),
        (name = "scorecards", description = "Derived scorecards"),
        (name = "search", description = "Ranked catalog search"),
        (name = "series", description = "Series metadata lookups"),
        (name = "subscriptions", description = "Webhook subscriptions")
    )
)]
pub struct ApiDoc;

#[derive(Debug)]
struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};

        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "ApiKeyAuth",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("X-API-Key"))),
        );
    }
}
