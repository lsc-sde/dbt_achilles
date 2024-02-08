-- 906	Distribution of age by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT
  de.drug_concept_id AS subject_id,
  p.gender_concept_id,
  de.drug_start_year - p.year_of_birth AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p
INNER JOIN (
  SELECT
    de.person_id,
    de.drug_concept_id,
    MIN(YEAR(de.drug_era_start_date)) AS drug_start_year
  FROM
    {{ ref(  var("achilles_source_schema") + "__drug_era" ) }} AS de
  INNER JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      de.person_id = op.person_id
      AND
      de.drug_era_start_date >= op.observation_period_start_date
      AND
      de.drug_era_start_date <= op.observation_period_end_date
  GROUP BY
    de.person_id,
    de.drug_concept_id
) AS de
  ON
    p.person_id = de.person_id
