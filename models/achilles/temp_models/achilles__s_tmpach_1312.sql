-- 1312	Number of persons with at least one visit detail by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    p.gender_concept_id AS stratum_2,
    YEAR(vd.visit_detail_start_date) AS stratum_1,
    FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
      AS stratum_3,
    count(DISTINCT vd.person_id) AS count_value
  FROM
    {{ source("omop", "person" ) }} AS p
    JOIN
    {{ source("omop", "visit_detail" ) }} AS vd
    ON
      vd.person_id = p.person_id
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      vd.person_id = op.person_id
      AND
      vd.visit_detail_start_date >= op.observation_period_start_date
      AND
      vd.visit_detail_start_date <= op.observation_period_end_date
  GROUP BY
    YEAR(vd.visit_detail_start_date),
    p.gender_concept_id,
    FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
)
SELECT
  1312 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
