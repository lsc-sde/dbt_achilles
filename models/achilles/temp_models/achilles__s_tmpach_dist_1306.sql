--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  analysis_id,
  stratum1_id AS stratum_1,
  stratum2_id AS stratum_2,
  count_value,
  min_value,
  max_value,
  avg_value,
  stdev_value,
  median_value,
  p10_value,
  p25_value,
  p75_value,
  p90_value,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  {{ ref ("achilles__tempResults_1306") }}
