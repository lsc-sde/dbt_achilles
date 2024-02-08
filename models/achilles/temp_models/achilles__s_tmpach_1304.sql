-- 1304	Number of persons with at least one visit detail, by visit_detail_concept_id by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    vd.visit_detail_concept_id AS stratum_1,
    p.gender_concept_id AS stratum_3,
    YEAR(vd.visit_detail_start_date) AS stratum_2,
    FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
      AS stratum_4,
    COUNT_BIG(DISTINCT p.person_id) AS count_value
  FROM
    {{ source("omop", "person" ) }} AS p
    JOIN
    {{ source("omop", "visit_detail" ) }} AS vd
    ON
      p.person_id = vd.person_id
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
    YEAR(vd.visit_detail_start_date),
    p.gender_concept_id,
    FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
)
SELECT
  1304 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(stratum_4 AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
