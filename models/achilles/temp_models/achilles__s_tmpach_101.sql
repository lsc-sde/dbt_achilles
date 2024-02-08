-- 101	Number of persons by age, with age at first observation period
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    year(op1.index_date) - p1.YEAR_OF_BIRTH AS stratum_1,
    count_big(p1.person_id) AS count_value
  FROM {{ source("omop", "person" ) }} AS p1
  INNER JOIN
    (SELECT
      person_id,
      min(observation_period_start_date) AS index_date
    FROM {{ source("omop", "observation_period" ) }} GROUP BY PERSON_ID) AS op1
    ON p1.PERSON_ID = op1.PERSON_ID
  GROUP BY year(op1.index_date) - p1.YEAR_OF_BIRTH
)
SELECT
  101 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(null AS VARCHAR(255)) AS stratum_2,
  CAST(null AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5
FROM rawData
