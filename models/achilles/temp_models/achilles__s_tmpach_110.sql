-- 110	Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  110 AS analysis_id,
  CAST(t1.obs_month AS VARCHAR(255)) AS stratum_1,
  CAST(null AS VARCHAR(255)) AS stratum_2,
  CAST(null AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(DISTINCT op1.PERSON_ID) AS count_value
FROM
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op1
INNER JOIN
  (
    SELECT DISTINCT
      YEAR(observation_period_start_date) * 100
      + MONTH(observation_period_start_date) AS obs_month,
      DATEFROMPARTS(
        YEAR(observation_period_start_date),
        MONTH(observation_period_start_date),
        1
      )
        AS obs_month_start,
      EOMONTH(observation_period_start_date) AS obs_month_end
    FROM {{ ref(  var("achilles_source_schema") + "__observation_period" ) }}
  ) AS t1
  ON
    op1.observation_period_start_date <= t1.obs_month_start
    AND op1.observation_period_end_date >= t1.obs_month_end
GROUP BY t1.obs_month