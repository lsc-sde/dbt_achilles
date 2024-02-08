-- 1820	Number of measurement records  by measurement start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date) AS stratum_1,
    COUNT_BIG(m.person_id) AS count_value
  FROM
    {{ source("omop", "measurement" ) }} AS m
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      m.person_id = op.person_id
      AND
      m.measurement_date >= op.observation_period_start_date
      AND
      m.measurement_date <= op.observation_period_end_date
  GROUP BY
    YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date)
)
SELECT
  1820 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
