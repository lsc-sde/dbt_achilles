-- 604	Number of persons with at least one procedure occurrence, by procedure_concept_id by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    po.procedure_concept_id AS stratum_1,
    p.gender_concept_id AS stratum_3,
    YEAR(po.procedure_date) AS stratum_2,
    FLOOR((YEAR(po.procedure_date) - p.year_of_birth) / 10) AS stratum_4,
    COUNT_BIG(DISTINCT p.person_id) AS count_value
  FROM
    {{ source("omop", "person" ) }} AS p
    JOIN
    {{ source("omop", "procedure_occurrence" ) }} AS po
    ON
      p.person_id = po.person_id
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      po.person_id = op.person_id
      AND
      po.procedure_date >= op.observation_period_start_date
      AND
      po.procedure_date <= op.observation_period_end_date
  GROUP BY
    po.procedure_concept_id,
    YEAR(po.procedure_date),
    p.gender_concept_id,
    FLOOR((YEAR(po.procedure_date) - p.year_of_birth) / 10)
)
SELECT
  604 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(stratum_4 AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
