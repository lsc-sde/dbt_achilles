-- 2191	Number of total persons that have at least x measurements
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  2191 AS analysis_id,
  CAST(d.device_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(d.device_count AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  SUM(COUNT(d.person_id))
    OVER (PARTITION BY d.device_concept_id ORDER BY d.device_count DESC)
    AS count_value
FROM (
  SELECT
    d.device_concept_id,
    d.person_id,
    COUNT(d.device_exposure_id) AS device_count
  FROM
    {{ source("omop", "device_exposure" ) }} AS d
  INNER JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      d.person_id = op.person_id
      AND
      d.device_exposure_start_date >= op.observation_period_start_date
      AND
      d.device_exposure_start_date <= op.observation_period_end_date
  GROUP BY
    d.person_id,
    d.device_concept_id
) AS d
GROUP BY
  d.device_concept_id,
  d.device_count
