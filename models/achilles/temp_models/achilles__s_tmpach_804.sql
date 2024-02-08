-- 804	Number of persons with at least one observation occurrence, by observation_concept_id by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    o.observation_concept_id AS stratum_1,
    p.gender_concept_id AS stratum_3,
    YEAR(o.observation_date) AS stratum_2,
    FLOOR((YEAR(o.observation_date) - p.year_of_birth) / 10) AS stratum_4,
    count(DISTINCT p.person_id) AS count_value
  FROM
    {{ source("omop", "person" ) }} AS p
    JOIN
    {{ source("omop", "observation" ) }} AS o
    ON
      p.person_id = o.person_id
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      o.person_id = op.person_id
      AND
      o.observation_date >= op.observation_period_start_date
      AND
      o.observation_date <= op.observation_period_end_date
  GROUP BY
    o.observation_concept_id,
    YEAR(o.observation_date),
    p.gender_concept_id,
    FLOOR((YEAR(o.observation_date) - p.year_of_birth) / 10)
)
SELECT
  804 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(stratum_4 AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
