-- 1807	Number of measurement occurrence records, by measurement_concept_id and unit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1807 AS analysis_id,
  CAST(m.measurement_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(m.unit_concept_id AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  count(m.person_id) AS count_value
FROM
  {{ source("omop", "measurement" ) }} AS m
INNER JOIN
  {{ source("omop", "observation_period" ) }} AS op
  ON
    m.person_id = op.person_id
    AND
    m.measurement_date >= op.observation_period_start_date
    AND
    m.measurement_date <= op.observation_period_end_date
GROUP BY
  m.measurement_concept_id,
  m.unit_concept_id
