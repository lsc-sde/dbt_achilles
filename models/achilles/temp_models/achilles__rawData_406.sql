-- 406	Distribution of age by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  c.condition_concept_id AS subject_id,
  p.gender_concept_id,
  (c.condition_start_year - p.year_of_birth) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p
INNER JOIN (
  SELECT
    co.person_id,
    co.condition_concept_id,
    MIN(YEAR(co.condition_start_date)) AS condition_start_year
  FROM
    {{ ref(  var("achilles_source_schema") + "__condition_occurrence" ) }} AS co
  INNER JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      co.person_id = op.person_id
      AND
      co.condition_start_date >= op.observation_period_start_date
      AND
      co.condition_start_date <= op.observation_period_end_date
  GROUP BY
    co.person_id,
    co.condition_concept_id
) AS c
  ON
    p.person_id = c.person_id
