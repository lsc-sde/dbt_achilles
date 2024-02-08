-- 625	Number of procedure_occurrence records, by procedure_source_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  625 AS analysis_id,
  CAST(po.procedure_source_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  count(*) AS count_value
FROM
  {{ source("omop", "procedure_occurrence" ) }} AS po
INNER JOIN
  {{ source("omop", "observation_period" ) }} AS op
  ON
    po.person_id = op.person_id
    AND
    po.procedure_date >= op.observation_period_start_date
    AND
    po.procedure_date <= op.observation_period_end_date
GROUP BY
  po.procedure_source_concept_id
