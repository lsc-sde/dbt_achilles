-- 2000	patients with at least 1 Dx and 1 Rx
SELECT
  2000 AS analysis_id,
  CAST(NULL AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  CAST(d.cnt AS BIGINT) AS count_value
FROM (
  SELECT count(*) AS cnt
  FROM (
    SELECT DISTINCT person_id
    FROM (
      SELECT co.person_id
      FROM
        {{ source("omop", "condition_occurrence" ) }} AS co
      INNER JOIN
        {{ source("omop", "observation_period" ) }} AS op
        ON
          co.person_id = op.person_id
          AND
          co.condition_start_date >= op.observation_period_start_date
          AND
          co.condition_start_date <= op.observation_period_end_date
    ) AS a
    INTERSECT
    SELECT DISTINCT person_id
    FROM (
      SELECT de.person_id
      FROM
        {{ source("omop", "drug_exposure" ) }} AS de
      INNER JOIN
        {{ source("omop", "observation_period" ) }} AS op
        ON
          de.person_id = op.person_id
          AND
          de.drug_exposure_start_date >= op.observation_period_start_date
          AND
          de.drug_exposure_start_date <= op.observation_period_end_date
    ) AS b
  ) AS c
) AS d
