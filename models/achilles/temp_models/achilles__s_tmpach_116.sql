--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    t1.obs_year AS stratum_1,
    p1.gender_concept_id AS stratum_2,
    floor((t1.obs_year - p1.year_of_birth) / 10) AS stratum_3,
    count(DISTINCT p1.PERSON_ID) AS count_value
  FROM
    {{ source("omop", "person" ) }} AS p1
  INNER JOIN
    {{ source("omop", "observation_period" ) }} AS op1
    ON p1.person_id = op1.person_id,
    {{ ref ("achilles__temp_dates_116") }} AS t1
  WHERE
    year(op1.OBSERVATION_PERIOD_START_DATE) <= t1.obs_year
    AND year(op1.OBSERVATION_PERIOD_END_DATE) >= t1.obs_year
  GROUP BY
    t1.obs_year,
    p1.gender_concept_id,
    floor((t1.obs_year - p1.year_of_birth) / 10)
)
SELECT
  116 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5
FROM rawData
