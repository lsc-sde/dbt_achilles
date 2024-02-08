--HINT DISTRIBUTE_ON_KEY(stratum1_id)
SELECT
  o.subject_id AS stratum1_id,
  o.unit_concept_id AS stratum2_id,
  o.count_value,
  COUNT_BIG(*) AS total,
  ROW_NUMBER() OVER (
    PARTITION BY o.subject_id, o.unit_concept_id ORDER BY o.count_value
  ) AS rn
FROM (
  SELECT
    o.observation_concept_id AS subject_id,
    o.unit_concept_id,
    CAST(o.value_as_number AS FLOAT) AS count_value
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
  WHERE
    o.unit_concept_id IS NOT NULL
    AND
    o.value_as_number IS NOT NULL
) AS o
GROUP BY
  o.subject_id,
  o.unit_concept_id,
  o.count_value
