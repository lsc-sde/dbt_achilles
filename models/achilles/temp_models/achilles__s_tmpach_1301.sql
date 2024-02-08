-- 1301	Number of visit detail records, by visit_detail_concept_id
-- restricted to visits overlapping with observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1301 AS analysis_id,
  CAST(vd.visit_detail_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(vd.person_id) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
INNER JOIN
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
  ON
    vd.person_id = op.person_id
    AND
    vd.visit_detail_start_date >= op.observation_period_start_date
    AND
    vd.visit_detail_start_date <= op.observation_period_end_date
GROUP BY
  vd.visit_detail_concept_id
