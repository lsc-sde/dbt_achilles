-- 1806	Distribution of age by measurement_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  o.measurement_concept_id AS subject_id,
  p.gender_concept_id,
  o.measurement_start_year - p.year_of_birth AS count_value
FROM
  {{ source("omop", "person" ) }} AS p
INNER JOIN (
  SELECT
    m.person_id,
    m.measurement_concept_id,
    MIN(YEAR(m.measurement_date)) AS measurement_start_year
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
    m.person_id,
    m.measurement_concept_id
) AS o
  ON
    p.person_id = o.person_id
