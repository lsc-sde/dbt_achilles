-- 701	Number of drug occurrence records, by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  701 AS analysis_id,
  CAST(de.drug_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(de.person_id) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__drug_exposure" ) }} AS de
INNER JOIN
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
  ON
    de.person_id = op.person_id
    AND
    de.drug_exposure_start_date >= op.observation_period_start_date
    AND
    de.drug_exposure_start_date <= op.observation_period_end_date
GROUP BY
  de.drug_concept_id
