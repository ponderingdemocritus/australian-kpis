export {
  ApiRequestError,
  ApiValidationError,
  createClient,
  type AuKpisClient,
  type CreateClientOptions,
  type ObservationLatestParams,
  type ObservationsListParams,
  type ScorecardHistoryParams,
} from './client.js'
export type {
  CoverageStatus,
  IndicatorContribution,
  SearchCatalogParams,
  SearchResponse,
  SearchResult,
  ScoreZone,
  ScorecardConfig,
  ScorecardSnapshot,
  SubIndexScore,
} from '@au-kpis/sdk-generated/client'
