-- 601	Number of procedure occurrence records, by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  601 AS analysis_id,
  CAST(po.procedure_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(po.person_id) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__procedure_occurrence" ) }} AS po
INNER JOIN
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
  ON
    po.person_id = op.person_id
    AND
    po.procedure_date >= op.observation_period_start_date
    AND
    po.procedure_date <= op.observation_period_end_date
GROUP BY
  po.procedure_concept_id
