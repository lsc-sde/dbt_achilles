-- 806	Distribution of age by observation_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  o.observation_concept_id AS subject_id,
  p.gender_concept_id,
  o.observation_start_year - p.year_of_birth AS count_value
FROM
  {{ source("omop", "person" ) }} AS p
INNER JOIN (
  SELECT
    o.person_id,
    o.observation_concept_id,
    MIN(YEAR(o.observation_date)) AS observation_start_year
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
    o.person_id,
    o.observation_concept_id
) AS o
  ON
    p.person_id = o.person_id
