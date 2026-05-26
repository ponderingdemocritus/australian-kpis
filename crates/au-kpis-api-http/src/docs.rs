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
        list_observations
    ),
    components(schemas(
        au_kpis_domain::Code,
        au_kpis_domain::Codelist,
        au_kpis_domain::Dataflow,
        au_kpis_domain::Dimension,
        DataflowCodelistResponse,
        DataflowDetailResponse,
        DataflowsQuery,
        DataflowsResponse,
        HealthResponse,
        ObservationsMetadata,
        ObservationsResponse,
        ObservationsRow,
        PaginationMetadata,
        ProblemDetails
    )),
    tags(
        (name = "dataflows", description = "Dataflow catalog and codelists"),
        (name = "observations", description = "Time-series observations")
    )
)]
pub struct ApiDoc;
