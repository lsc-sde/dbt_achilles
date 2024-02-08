-- 1302	Number of persons by visit detail start month, by visit_detail_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    vd.visit_detail_concept_id AS stratum_1,
    YEAR(vd.visit_detail_start_date) * 100
    + MONTH(vd.visit_detail_start_date) AS stratum_2,
    count(DISTINCT vd.person_id) AS count_value
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
    vd.visit_detail_concept_id,
    YEAR(vd.visit_detail_start_date) * 100 + MONTH(vd.visit_detail_start_date)
)
SELECT
  1302 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
