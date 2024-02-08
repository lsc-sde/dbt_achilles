-- 1321	Number of persons by visit start year
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    YEAR(vd.visit_detail_start_date) AS stratum_1,
    COUNT_BIG(DISTINCT vd.person_id) AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      vd.person_id = op.person_id
      AND
      vd.visit_detail_start_date >= op.observation_period_start_date
      AND
      vd.visit_detail_start_date <= op.observation_period_end_date
  GROUP BY
    YEAR(vd.visit_detail_start_date)
)
SELECT
  1321 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData