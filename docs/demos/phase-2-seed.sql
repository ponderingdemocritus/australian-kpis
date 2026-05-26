INSERT INTO sources (id, name, homepage, description)
VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    homepage = EXCLUDED.homepage,
    description = EXCLUDED.description;

INSERT INTO codelists (id, name, description)
VALUES
  ('CL_REGION_AU', 'Australian regions', 'ABS statistical regions'),
  ('CL_MEASURE_CPI', 'CPI measures', 'Consumer Price Index measures')
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description;

INSERT INTO codes (codelist_id, id, name, description, parent_id)
VALUES
  ('CL_REGION_AU', 'AUS', 'Australia', NULL, NULL),
  ('CL_REGION_AU', 'NSW', 'New South Wales', NULL, 'AUS'),
  ('CL_REGION_AU', 'VIC', 'Victoria', NULL, 'AUS'),
  ('CL_REGION_AU', 'QLD', 'Queensland', NULL, 'AUS'),
  ('CL_REGION_AU', 'SA', 'South Australia', NULL, 'AUS'),
  ('CL_REGION_AU', 'WA', 'Western Australia', NULL, 'AUS'),
  ('CL_REGION_AU', 'TAS', 'Tasmania', NULL, 'AUS'),
  ('CL_REGION_AU', 'NT', 'Northern Territory', NULL, 'AUS'),
  ('CL_REGION_AU', 'ACT', 'Australian Capital Territory', NULL, 'AUS'),
  ('CL_MEASURE_CPI', 'index', 'CPI index', NULL, NULL)
ON CONFLICT (codelist_id, id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description,
    parent_id = EXCLUDED.parent_id;

INSERT INTO measures (id, name, description, unit, scale)
VALUES ('index', 'CPI index', NULL, 'index', NULL)
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
    'Quarterly Consumer Price Index across Australian regions.',
    ARRAY['region', 'measure'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
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

INSERT INTO dimensions (dataflow_id, id, name, description, codelist_id, position)
VALUES
  ('abs.cpi', 'region', 'Region', 'Geographic region', 'CL_REGION_AU', 0),
  ('abs.cpi', 'measure', 'Measure', 'Observation measure', 'CL_MEASURE_CPI', 1)
ON CONFLICT (dataflow_id, id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description,
    codelist_id = EXCLUDED.codelist_id,
    position = EXCLUDED.position;
