-- 404	Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    co.condition_concept_id AS stratum_1,
    p.gender_concept_id AS stratum_3,
    YEAR(co.condition_start_date) AS stratum_2,
    FLOOR((YEAR(co.condition_start_date) - p.year_of_birth) / 10) AS stratum_4,
    count(DISTINCT p.person_id) AS count_value
  FROM
    {{ source("omop", "person" ) }} AS p
    JOIN
    {{ source("omop", "condition_occurrence" ) }} AS co
    ON
      p.person_id = co.person_id
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      co.person_id = op.person_id
      AND
      co.condition_start_date >= op.observation_period_start_date
      AND
      co.condition_start_date <= op.observation_period_end_date
  GROUP BY
    co.condition_concept_id,
    YEAR(co.condition_start_date),
    p.gender_concept_id,
    FLOOR((YEAR(co.condition_start_date) - p.year_of_birth) / 10)
)
SELECT
  404 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(stratum_4 AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
