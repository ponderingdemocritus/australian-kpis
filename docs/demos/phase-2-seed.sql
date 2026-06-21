INSERT INTO sources (id, name, homepage, description)
VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    homepage = EXCLUDED.homepage,
    description = EXCLUDED.description;

INSERT INTO codelists (id, name, description)
VALUES
  ('CL_ABS_CPI_REGION', 'ABS CPI regions', 'ABS CPI capital-city and national regions'),
  ('CL_ABS_CPI_MEASURE', 'ABS CPI measures', 'ABS CPI measure types'),
  ('CL_ABS_CPI_INDEX', 'ABS CPI expenditure classes', 'ABS CPI index categories'),
  ('CL_ABS_CPI_TSEST', 'ABS CPI adjustment types', 'Original and seasonally adjusted series'),
  ('CL_ABS_CPI_FREQ', 'ABS CPI frequencies', 'Monthly and quarterly frequencies')
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description;

DELETE FROM codes
WHERE codelist_id IN (
  'CL_ABS_CPI_REGION',
  'CL_ABS_CPI_MEASURE',
  'CL_ABS_CPI_INDEX',
  'CL_ABS_CPI_TSEST',
  'CL_ABS_CPI_FREQ'
);

INSERT INTO codes (codelist_id, id, name, description, parent_id)
VALUES
  ('CL_ABS_CPI_REGION', '50', 'Australia', NULL, NULL),
  ('CL_ABS_CPI_REGION', '1', 'Sydney', NULL, NULL),
  ('CL_ABS_CPI_REGION', '2', 'Melbourne', NULL, NULL),
  ('CL_ABS_CPI_REGION', '3', 'Brisbane', NULL, NULL),
  ('CL_ABS_CPI_REGION', '4', 'Adelaide', NULL, NULL),
  ('CL_ABS_CPI_REGION', '5', 'Perth', NULL, NULL),
  ('CL_ABS_CPI_REGION', '6', 'Hobart', NULL, NULL),
  ('CL_ABS_CPI_REGION', '7', 'Darwin', NULL, NULL),
  ('CL_ABS_CPI_REGION', '8', 'Canberra', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '1', 'Index numbers', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '2', 'Percentage change from previous period', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '3', 'Percentage change from previous year', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '4', 'Contribution to CPI index number', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '5', 'Change in contribution to CPI index number', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '6', 'Contribution to percentage points change from previous period', NULL, NULL),
  ('CL_ABS_CPI_MEASURE', '7', 'Contribution to Annual percentage points change', NULL, NULL),
  ('CL_ABS_CPI_INDEX', '10001', 'All groups CPI', NULL, NULL),
  ('CL_ABS_CPI_TSEST', '10', 'Original', NULL, NULL),
  ('CL_ABS_CPI_TSEST', '20', 'Seasonally Adjusted', NULL, NULL),
  ('CL_ABS_CPI_FREQ', 'M', 'Monthly', NULL, NULL),
  ('CL_ABS_CPI_FREQ', 'Q', 'Quarterly', NULL, NULL)
ON CONFLICT (codelist_id, id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description,
    parent_id = EXCLUDED.parent_id;

INSERT INTO measures (id, name, description, unit, scale)
VALUES
  ('1', 'Index numbers', NULL, 'index', NULL),
  ('2', 'Percentage change from previous period', NULL, 'percent', NULL),
  ('3', 'Percentage change from previous year', NULL, 'percent', NULL),
  ('4', 'Contribution to CPI index number', NULL, 'index points', NULL),
  ('5', 'Change in contribution to CPI index number', NULL, 'index points', NULL),
  ('6', 'Contribution to percentage points change from previous period', NULL, 'percentage points', NULL),
  ('7', 'Contribution to Annual percentage points change', NULL, 'percentage points', NULL)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description,
    unit = EXCLUDED.unit,
    scale = EXCLUDED.scale;

INSERT INTO dataflows (
    id, source_id, name, description, dimensions, measures,
    frequency, license, attribution, source_url
)
VALUES (
    'abs.cpi', 'abs', 'Consumer Price Index',
    'Consumer Price Index by measure, expenditure class, adjustment type, region, and frequency.',
    ARRAY['measure', 'index', 'tsest', 'region', 'freq'],
    ARRAY['1', '2', '3', '4', '5', '6', '7'],
    'quarterly', 'CC-BY-4.0',
    'Source: Australian Bureau of Statistics',
    'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia'
)
ON CONFLICT (id) DO UPDATE
SET source_id = EXCLUDED.source_id,
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    dimensions = EXCLUDED.dimensions,
    measures = EXCLUDED.measures,
    frequency = EXCLUDED.frequency,
    license = EXCLUDED.license,
    attribution = EXCLUDED.attribution,
    source_url = EXCLUDED.source_url;

DELETE FROM dimensions
WHERE dataflow_id = 'abs.cpi';

INSERT INTO dimensions (dataflow_id, id, name, description, codelist_id, position)
VALUES
  ('abs.cpi', 'measure', 'Measure', 'Observation measure', 'CL_ABS_CPI_MEASURE', 0),
  ('abs.cpi', 'index', 'Index', 'ABS CPI expenditure class', 'CL_ABS_CPI_INDEX', 1),
  ('abs.cpi', 'tsest', 'Adjustment Type', 'ABS seasonal adjustment type', 'CL_ABS_CPI_TSEST', 2),
  ('abs.cpi', 'region', 'Region', 'ABS CPI capital-city or national region', 'CL_ABS_CPI_REGION', 3),
  ('abs.cpi', 'freq', 'Frequency', 'ABS CPI publication frequency', 'CL_ABS_CPI_FREQ', 4)
ON CONFLICT (dataflow_id, id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description,
    codelist_id = EXCLUDED.codelist_id,
    position = EXCLUDED.position;
