-- 1320	Number of visit detail records by visit detail start month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    YEAR(vd.visit_detail_start_date) * 100
    + MONTH(vd.visit_detail_start_date) AS stratum_1,
    count(vd.person_id) AS count_value
  FROM
    {{ source("omop", "visit_detail" ) }} AS vd
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      vd.person_id = op.person_id
      AND
      vd.visit_detail_start_date >= op.observation_period_start_date
      AND
      vd.visit_detail_start_date <= op.observation_period_end_date
  GROUP BY
    YEAR(vd.visit_detail_start_date) * 100 + MONTH(vd.visit_detail_start_date)
)
SELECT
  1320 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
