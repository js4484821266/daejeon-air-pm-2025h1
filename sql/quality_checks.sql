-- 1) raw_pm row exists
SELECT
  'raw_has_rows' AS check_name,
  (COUNT(*) > 0) AS passed,
  ('count=' || COUNT(*)) AS details
FROM raw_pm;

-- 2) measured_at not null
SELECT
  'raw_measured_at_not_null' AS check_name,
  (COUNT(*) = 0) AS passed,
  ('null_count=' || COUNT(*)) AS details
FROM raw_pm
WHERE measured_at IS NULL;

-- 3) station not null/empty
SELECT
  'raw_station_not_empty' AS check_name,
  (COUNT(*) = 0) AS passed,
  ('bad_count=' || COUNT(*)) AS details
FROM raw_pm
WHERE station IS NULL OR BTRIM(station) = '';

-- 4) pm values non-negative (allow NULL)
SELECT
  'raw_pm_non_negative' AS check_name,
  (COUNT(*) = 0) AS passed,
  ('bad_count=' || COUNT(*)) AS details
FROM raw_pm
WHERE (pm10 IS NOT NULL AND pm10 < 0)
   OR (pm25 IS NOT NULL AND pm25 < 0);

-- 5) mart exists after transform
SELECT
  'mart_has_rows' AS check_name,
  (COUNT(*) > 0) AS passed,
  ('count=' || COUNT(*)) AS details
FROM mart_daily_summary;
