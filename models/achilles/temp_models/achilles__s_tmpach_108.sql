-- 108	Number of persons by length of observation period, in 30d increments
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    floor(
      datediff(
        DD, op1.observation_period_start_date, op1.observation_period_end_date
      )
      / 30
    ) AS stratum_1,
    count(DISTINCT p1.person_id) AS count_value
  FROM {{ source("omop", "person" ) }} AS p1
  INNER JOIN
    (
      SELECT
        person_id,
        OBSERVATION_PERIOD_START_DATE,
        OBSERVATION_PERIOD_END_DATE,
        ROW_NUMBER() OVER (
          PARTITION BY person_id ORDER BY observation_period_start_date ASC
        ) AS rn1
      FROM {{ source("omop", "observation_period" ) }}
    ) AS op1
    ON p1.PERSON_ID = op1.PERSON_ID
  WHERE op1.rn1 = 1
  GROUP BY
    floor(
      datediff(
        DD, op1.observation_period_start_date, op1.observation_period_end_date
      )
      / 30
    )
)
SELECT
  108 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(null AS VARCHAR(255)) AS stratum_2,
  CAST(null AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5
FROM rawData
