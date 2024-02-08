-- 1891	Number of total persons that have at least x measurements
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1891 AS analysis_id,
  CAST(m.measurement_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(m.meas_cnt AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  SUM(COUNT(m.person_id))
    OVER (PARTITION BY m.measurement_concept_id ORDER BY m.meas_cnt DESC)
    AS count_value
FROM (
  SELECT
    m.measurement_concept_id,
    m.person_id,
    COUNT(m.measurement_id) AS meas_cnt
  FROM
    {{ source("omop", "measurement" ) }} AS m
  INNER JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      m.person_id = op.person_id
      AND
      m.measurement_date >= op.observation_period_start_date
      AND
      m.measurement_date <= op.observation_period_end_date
  GROUP BY
    m.person_id,
    m.measurement_concept_id
) AS m
GROUP BY
  m.measurement_concept_id,
  m.meas_cnt
