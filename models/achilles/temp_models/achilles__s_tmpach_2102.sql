-- 2102	Number of persons by device by  start month, by device_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    de.device_concept_id AS stratum_1,
    YEAR(de.device_exposure_start_date) * 100
    + MONTH(de.device_exposure_start_date) AS stratum_2,
    count(DISTINCT de.person_id) AS count_value
  FROM
    {{ source("omop", "device_exposure" ) }} AS de
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      de.person_id = op.person_id
      AND
      de.device_exposure_start_date >= op.observation_period_start_date
      AND
      de.device_exposure_start_date <= op.observation_period_end_date
  GROUP BY
    de.device_concept_id,
    YEAR(de.device_exposure_start_date) * 100
    + MONTH(de.device_exposure_start_date)
)
SELECT
  2102 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM
  rawData
