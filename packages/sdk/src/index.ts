export {
  ApiRequestError,
  ApiValidationError,
  createClient,
  type AuKpisClient,
  type CreateClientOptions,
  type ObservationLatestParams,
  type ObservationsListParams,
  type ScorecardConfigParams,
  type ScorecardHistoryParams,
  type ScorecardLatestParams,
} from './client.js'
export type {
  ApsSnapshotSummary,
  CoverageStatus,
  IndicatorContribution,
  SearchCatalogParams,
  SearchResponse,
  SearchResult,
  ScoreZone,
  PublishedApsSnapshot,
  ScorecardConfig,
  SubIndexScore,
} from '@au-kpis/sdk-generated/client'
