--HINT DISTRIBUTE_ON_KEY(stratum1_id)
SELECT
  m.subject_id AS stratum1_id,
  m.unit_concept_id AS stratum2_id,
  m.count_value,
  COUNT_BIG(*) AS total,
  ROW_NUMBER() OVER (
    PARTITION BY m.subject_id, m.unit_concept_id ORDER BY m.count_value
  ) AS rn
FROM (
  SELECT
    m.measurement_concept_id AS subject_id,
    m.unit_concept_id,
    CAST(m.range_high AS FLOAT) AS count_value
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
    m.unit_concept_id IS NOT NULL
    AND
    m.value_as_number IS NOT NULL
    AND
    m.range_low IS NOT NULL
    AND
    m.range_high IS NOT NULL
) AS m
GROUP BY
  m.subject_id,
  m.unit_concept_id,
  m.count_value
