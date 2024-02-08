-- 1001	Number of condition occurrence records, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1001 AS analysis_id,
  CAST(ce.condition_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(ce.person_id) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__condition_era" ) }} AS ce
INNER JOIN
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
  ON
    ce.person_id = op.person_id
    AND
    ce.condition_era_start_date >= op.observation_period_start_date
    AND
    ce.condition_era_start_date <= op.observation_period_end_date
GROUP BY
  ce.condition_concept_id
