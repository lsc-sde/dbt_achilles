-- 225	Number of visit_occurrence records, by visit_source_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  225 AS analysis_id,
  CAST(vo.visit_source_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(*) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} AS vo
INNER JOIN
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
  ON
    vo.person_id = op.person_id
    AND
    vo.visit_start_date >= op.observation_period_start_date
    AND
    vo.visit_start_date <= op.observation_period_end_date
GROUP BY
  visit_source_concept_id
