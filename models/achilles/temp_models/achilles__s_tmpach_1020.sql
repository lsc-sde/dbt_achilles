-- 1020	Number of condition era records by condition era start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    YEAR(ce.condition_era_start_date) * 100
    + MONTH(ce.condition_era_start_date) AS stratum_1,
    COUNT_BIG(ce.person_id) AS count_value
  FROM
    {{ source("omop", "condition_era" ) }} AS ce
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      ce.person_id = op.person_id
      AND
      ce.condition_era_start_date >= op.observation_period_start_date
      AND
      ce.condition_era_start_date <= op.observation_period_end_date
  GROUP BY
    YEAR(ce.condition_era_start_date) * 100 + MONTH(ce.condition_era_start_date)
)
SELECT
  1020 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
