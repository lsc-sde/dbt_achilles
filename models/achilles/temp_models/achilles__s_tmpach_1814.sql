-- 1814	Number of measurement records with no value (numeric or concept)
SELECT
  1814 AS analysis_id,
  CAST(NULL AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
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
WHERE
  m.value_as_number IS NULL
  AND
  (m.value_as_concept_id IS NULL OR m.value_as_concept_id = 0)
