-- 112	Number of persons by observation period end month
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    YEAR(op1.observation_period_end_date) * 100
    + MONTH(op1.observation_period_end_date) AS stratum_1,
    COUNT_BIG(DISTINCT op1.PERSON_ID) AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op1
  GROUP BY
    YEAR(op1.observation_period_end_date) * 100
    + MONTH(op1.observation_period_end_date)
)
SELECT
  112 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(null AS VARCHAR(255)) AS stratum_2,
  CAST(null AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5
FROM rawData
