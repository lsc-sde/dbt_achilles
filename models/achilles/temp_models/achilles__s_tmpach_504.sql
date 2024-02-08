-- 504	Number of persons with a death, by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    p.gender_concept_id AS stratum_2,
    YEAR(d.death_date) AS stratum_1,
    FLOOR((YEAR(d.death_date) - p.year_of_birth) / 10) AS stratum_3,
    COUNT_BIG(DISTINCT p.person_id) AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p
    JOIN
    {{ ref(  var("achilles_source_schema") + "__death" ) }} AS d
    ON
      p.person_id = d.person_id
    JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      d.person_id = op.person_id
      AND
      d.death_date >= op.observation_period_start_date
      AND
      d.death_date <= op.observation_period_end_date
  GROUP BY
    YEAR(d.death_date),
    p.gender_concept_id,
    FLOOR((YEAR(d.death_date) - p.year_of_birth) / 10)
)
SELECT
  504 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
