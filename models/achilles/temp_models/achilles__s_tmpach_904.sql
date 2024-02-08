-- 904	Number of persons with at least one drug occurrence, by drug_concept_id by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    de.drug_concept_id AS stratum_1,
    p.gender_concept_id AS stratum_3,
    YEAR(de.drug_era_start_date) AS stratum_2,
    FLOOR((YEAR(de.drug_era_start_date) - p.year_of_birth) / 10) AS stratum_4,
    COUNT_BIG(DISTINCT p.person_id) AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p
    JOIN
    {{ ref(  var("achilles_source_schema") + "__drug_era" ) }} AS de
    ON
      p.person_id = de.person_id
    JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      de.person_id = op.person_id
      AND
      de.drug_era_start_date >= op.observation_period_start_date
      AND
      de.drug_era_start_date <= op.observation_period_end_date
  GROUP BY
    de.drug_concept_id,
    YEAR(de.drug_era_start_date),
    p.gender_concept_id,
    FLOOR((YEAR(de.drug_era_start_date) - p.year_of_birth) / 10)
)
SELECT
  904 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(stratum_4 AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
