INSERT INTO sources (id, name, homepage, description)
VALUES
  ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL),
  ('rba', 'Reserve Bank of Australia', 'https://www.rba.gov.au', NULL);

INSERT INTO codelists (id, name, description)
VALUES ('CL_REGION_AU', 'Australian regions', 'ABS statistical regions');

INSERT INTO codes (codelist_id, id, name, description, parent_id)
VALUES
  ('CL_REGION_AU', 'AUS', 'Australia', NULL, NULL),
  ('CL_REGION_AU', 'NSW', 'New South Wales', NULL, 'AUS'),
  ('CL_REGION_AU', 'VIC', 'Victoria', NULL, 'AUS'),
  ('CL_REGION_AU', 'QLD', 'Queensland', NULL, 'AUS');

INSERT INTO measures (id, name, description, unit, scale)
VALUES
  ('index', 'CPI index', NULL, 'index', NULL),
  ('rate', 'Policy interest rate', NULL, 'percent', NULL),
  ('people', 'Employed people', NULL, 'thousand persons', NULL);

INSERT INTO dataflows (
    id, source_id, name, description, dimensions, measures,
    frequency, license, attribution, source_url
)
VALUES
  (
      'abs.cpi', 'abs', 'Consumer Price Index',
      'Quarterly Consumer Price Index across Australian regions.',
      ARRAY['region'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
      'Source: Australian Bureau of Statistics',
      'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia'
  ),
  (
      'rba.cash_rate', 'rba', 'Cash Rate Target',
      'Monthly Reserve Bank of Australia cash rate target.',
      ARRAY[]::text[], ARRAY['rate'], 'monthly', 'CC-BY-4.0',
      'Source: Reserve Bank of Australia',
      'https://www.rba.gov.au/statistics/cash-rate/'
  ),
  (
      'abs.labour_force', 'abs', 'Labour Force',
      'Monthly employed people by Australian region.',
      ARRAY['region'], ARRAY['people'], 'monthly', 'CC-BY-4.0',
      'Source: Australian Bureau of Statistics',
      'https://www.abs.gov.au/statistics/labour/employment-and-unemployment/labour-force-australia'
  );

INSERT INTO dimensions (dataflow_id, id, name, description, codelist_id, position)
VALUES
  ('abs.cpi', 'region', 'Region', 'Geographic region', 'CL_REGION_AU', 0),
  ('abs.labour_force', 'region', 'Region', 'Geographic region', 'CL_REGION_AU', 0);

INSERT INTO artifacts (
    id, source_id, source_url, content_type, response_headers,
    size_bytes, storage_key, fetched_at
)
VALUES
  (
      decode(repeat('e', 64), 'hex'), 'abs',
      'https://example.test/cpi.json', 'application/json',
      '{}'::jsonb, 512, 'artifacts/web-explorer-fixture',
      '2024-07-24T00:00:00Z'
  ),
  (
      decode(repeat('f', 64), 'hex'), 'rba',
      'https://example.test/cash-rate.json', 'application/json',
      '{}'::jsonb, 384, 'artifacts/web-cash-rate-fixture',
      '2024-07-24T00:00:00Z'
  ),
  (
      decode(repeat('1', 64), 'hex'), 'abs',
      'https://example.test/labour-force.json', 'application/json',
      '{}'::jsonb, 640, 'artifacts/web-labour-force-fixture',
      '2024-07-24T00:00:00Z'
  );

INSERT INTO series (
    series_key, dataflow_id, measure_id, dimensions, unit,
    first_observed, last_observed, active
)
VALUES
  (
    decode(repeat('a', 64), 'hex'), 'abs.cpi', 'index',
    '{"region":"AUS"}'::jsonb, 'index',
    '2023-09-01T00:00:00Z', '2024-06-01T00:00:00Z', true
  ),
  (
    decode(repeat('b', 64), 'hex'), 'abs.cpi', 'index',
    '{"region":"NSW"}'::jsonb, 'index',
    '2023-09-01T00:00:00Z', '2024-06-01T00:00:00Z', true
  ),
  (
    decode(repeat('c', 64), 'hex'), 'abs.cpi', 'index',
    '{"region":"VIC"}'::jsonb, 'index',
    '2023-09-01T00:00:00Z', '2024-06-01T00:00:00Z', true
  ),
  (
    decode(repeat('d', 64), 'hex'), 'abs.cpi', 'index',
    '{"region":"QLD"}'::jsonb, 'index',
    '2023-09-01T00:00:00Z', '2024-06-01T00:00:00Z', true
  ),
  (
    decode(repeat('2', 64), 'hex'), 'rba.cash_rate', 'rate',
    '{}'::jsonb, 'percent',
    '2024-03-01T00:00:00Z', '2024-06-01T00:00:00Z', true
  ),
  (
    decode(repeat('3', 64), 'hex'), 'abs.labour_force', 'people',
    '{"region":"AUS"}'::jsonb, 'thousand persons',
    '2024-03-01T00:00:00Z', '2024-06-01T00:00:00Z', true
  );

INSERT INTO observations (
    series_key, time, revision_no, time_precision, value, status,
    attributes, ingested_at, source_artifact_id
)
VALUES
  (decode(repeat('a', 64), 'hex'), '2023-09-01T00:00:00Z', 0, 'quarter', 132.1, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('a', 64), 'hex'), '2023-12-01T00:00:00Z', 0, 'quarter', 134.4, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('a', 64), 'hex'), '2024-03-01T00:00:00Z', 0, 'quarter', 135.6, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('a', 64), 'hex'), '2024-06-01T00:00:00Z', 0, 'quarter', 136.9, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('b', 64), 'hex'), '2023-09-01T00:00:00Z', 0, 'quarter', 134.7, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('b', 64), 'hex'), '2023-12-01T00:00:00Z', 0, 'quarter', 136.8, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('b', 64), 'hex'), '2024-03-01T00:00:00Z', 0, 'quarter', 138.1, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('b', 64), 'hex'), '2024-06-01T00:00:00Z', 0, 'quarter', 139.2, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('c', 64), 'hex'), '2023-09-01T00:00:00Z', 0, 'quarter', 131.9, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('c', 64), 'hex'), '2023-12-01T00:00:00Z', 0, 'quarter', 133.0, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('c', 64), 'hex'), '2024-03-01T00:00:00Z', 0, 'quarter', 134.5, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('c', 64), 'hex'), '2024-06-01T00:00:00Z', 0, 'quarter', 135.4, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('d', 64), 'hex'), '2023-09-01T00:00:00Z', 0, 'quarter', 133.5, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('d', 64), 'hex'), '2023-12-01T00:00:00Z', 0, 'quarter', 135.2, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('d', 64), 'hex'), '2024-03-01T00:00:00Z', 0, 'quarter', 136.8, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('d', 64), 'hex'), '2024-06-01T00:00:00Z', 0, 'quarter', 138.0, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('e', 64), 'hex')),
  (decode(repeat('2', 64), 'hex'), '2024-03-01T00:00:00Z', 0, 'month', 4.35, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('f', 64), 'hex')),
  (decode(repeat('2', 64), 'hex'), '2024-04-01T00:00:00Z', 0, 'month', 4.35, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('f', 64), 'hex')),
  (decode(repeat('2', 64), 'hex'), '2024-05-01T00:00:00Z', 0, 'month', 4.35, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('f', 64), 'hex')),
  (decode(repeat('2', 64), 'hex'), '2024-06-01T00:00:00Z', 0, 'month', 4.35, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('f', 64), 'hex')),
  (decode(repeat('3', 64), 'hex'), '2024-03-01T00:00:00Z', 0, 'month', 14288.0, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('1', 64), 'hex')),
  (decode(repeat('3', 64), 'hex'), '2024-04-01T00:00:00Z', 0, 'month', 14321.0, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('1', 64), 'hex')),
  (decode(repeat('3', 64), 'hex'), '2024-05-01T00:00:00Z', 0, 'month', 14377.0, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('1', 64), 'hex')),
  (decode(repeat('3', 64), 'hex'), '2024-06-01T00:00:00Z', 0, 'month', 14423.0, 'normal', '{}'::jsonb, '2024-07-24T00:00:00Z', decode(repeat('1', 64), 'hex'));
