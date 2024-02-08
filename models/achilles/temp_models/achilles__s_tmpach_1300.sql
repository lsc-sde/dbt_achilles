-- 1300	Number of persons with at least one visit detail, by visit_detail_concept_id
-- restricted to visits overlapping with observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1300 AS analysis_id,
  CAST(vd.visit_detail_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  count(DISTINCT vd.person_id) AS count_value
FROM
  {{ source("omop", "visit_detail" ) }} AS vd
INNER JOIN
  {{ source("omop", "observation_period" ) }} AS op
  ON
    vd.person_id = op.person_id
    AND
    vd.visit_detail_start_date >= op.observation_period_start_date
    AND
    vd.visit_detail_start_date <= op.observation_period_end_date
GROUP BY
  vd.visit_detail_concept_id
