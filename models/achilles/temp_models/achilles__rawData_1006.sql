-- 1006	Distribution of age by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  ce.condition_concept_id AS subject_id,
  p.gender_concept_id,
  ce.condition_start_year - p.year_of_birth AS count_value
FROM
  {{ source("omop", "person" ) }} AS p
INNER JOIN (
  SELECT
    ce.person_id,
    ce.condition_concept_id,
    MIN(YEAR(ce.condition_era_start_date)) AS condition_start_year
  FROM
    {{ source("omop", "condition_era" ) }} AS ce
  INNER JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      ce.person_id = op.person_id
      AND
      ce.condition_era_start_date >= op.observation_period_start_date
      AND
      ce.condition_era_start_date <= op.observation_period_end_date
  GROUP BY
    ce.person_id,
    ce.condition_concept_id
) AS ce
  ON
    p.person_id = ce.person_id
