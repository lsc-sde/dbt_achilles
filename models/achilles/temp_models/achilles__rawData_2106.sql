-- 2106	Distribution of age by device_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  o.device_concept_id AS subject_id,
  p.gender_concept_id,
  o.device_exposure_start_year - p.year_of_birth AS count_value
FROM
  {{ source("omop", "person" ) }} AS p
INNER JOIN (
  SELECT
    d.person_id,
    d.device_concept_id,
    MIN(YEAR(d.device_exposure_start_date)) AS device_exposure_start_year
  FROM
    {{ source("omop", "device_exposure" ) }} AS d
  INNER JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      d.person_id = op.person_id
      AND
      d.device_exposure_start_date >= op.observation_period_start_date
      AND
      d.device_exposure_start_date <= op.observation_period_end_date
  GROUP BY
    d.person_id,
    d.device_concept_id
) AS o
  ON
    p.person_id = o.person_id
