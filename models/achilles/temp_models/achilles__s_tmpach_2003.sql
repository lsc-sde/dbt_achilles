SELECT
  2003 AS analysis_id,
  CAST(NULL AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(DISTINCT vo.person_id) AS count_value
FROM
  {{ source("omop", "visit_occurrence" ) }} AS vo
INNER JOIN
  {{ source("omop", "observation_period" ) }} AS op
  ON
    vo.person_id = op.person_id
    AND
    vo.visit_start_date >= op.observation_period_start_date
    AND
    vo.visit_start_date <= op.observation_period_end_date
