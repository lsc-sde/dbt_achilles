-- 606	Distribution of age by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  po.procedure_concept_id AS subject_id,
  p.gender_concept_id,
  po.procedure_start_year - p.year_of_birth AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p
INNER JOIN (
  SELECT
    po.person_id,
    po.procedure_concept_id,
    MIN(YEAR(po.procedure_date)) AS procedure_start_year
  FROM
    {{ ref(  var("achilles_source_schema") + "__procedure_occurrence" ) }} AS po
  INNER JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      po.person_id = op.person_id
      AND
      po.procedure_date >= op.observation_period_start_date
      AND
      po.procedure_date <= op.observation_period_end_date
  GROUP BY
    po.person_id,
    po.procedure_concept_id
) AS po
  ON
    p.person_id = po.person_id
