-- 2002	patients with at least 1 Mes and 1 Dx and 1 Rx
SELECT
  2002 AS analysis_id,
  CAST(NULL AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  CAST(e.cnt AS BIGINT) AS count_value
FROM (
  SELECT COUNT_BIG(*) AS cnt
  FROM (
    SELECT DISTINCT person_id
    FROM (
      SELECT m.person_id
      FROM
        {{ ref(  var("achilles_source_schema") + "__measurement" ) }} AS m
      INNER JOIN
        {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
        ON
          m.person_id = op.person_id
          AND
          m.measurement_date >= op.observation_period_start_date
          AND
          m.measurement_date <= op.observation_period_end_date
    ) AS a
    INTERSECT
    SELECT DISTINCT person_id
    FROM (
      SELECT co.person_id
      FROM
        {{ ref(  var("achilles_source_schema") + "__condition_occurrence" ) }} AS co
      INNER JOIN
        {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
        ON
          co.person_id = op.person_id
          AND
          co.condition_start_date >= op.observation_period_start_date
          AND
          co.condition_start_date <= op.observation_period_end_date
    ) AS b
    INTERSECT
    SELECT DISTINCT person_id
    FROM (
      SELECT de.person_id
      FROM
        {{ ref(  var("achilles_source_schema") + "__drug_exposure" ) }} AS de
      INNER JOIN
        {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
        ON
          de.person_id = op.person_id
          AND
          de.drug_exposure_start_date >= op.observation_period_start_date
          AND
          de.drug_exposure_start_date <= op.observation_period_end_date
    ) AS c
  ) AS d
) AS e