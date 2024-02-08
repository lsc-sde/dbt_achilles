-- 826	Number of observation records, by value_as_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  826 AS analysis_id,
  CAST(o.value_as_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  count(*) AS count_value
FROM
  {{ source("omop", "observation" ) }} AS o
INNER JOIN
  {{ source("omop", "observation_period" ) }} AS op
  ON
    o.person_id = op.person_id
    AND
    o.observation_date >= op.observation_period_start_date
    AND
    o.observation_date <= op.observation_period_end_date
GROUP BY
  o.value_as_concept_id
