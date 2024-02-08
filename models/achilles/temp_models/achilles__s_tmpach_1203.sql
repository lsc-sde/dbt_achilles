-- 1203	Number of visits by place of service discharge type
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1203 AS analysis_id,
  CAST(vo.discharged_to_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  count(*) AS count_value
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
WHERE
  vo.discharged_to_concept_id != 0
GROUP BY
  vo.discharged_to_concept_id
