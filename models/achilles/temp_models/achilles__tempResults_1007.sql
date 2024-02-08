-- 1007	Distribution of condition era length, by condition_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData (stratum1_id, count_value) AS (
  SELECT
    ce.condition_concept_id AS stratum1_id,
    DATEDIFF(DD, ce.condition_era_start_date, ce.condition_era_end_date)
      AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__condition_era" ) }} AS ce
    JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      ce.person_id = op.person_id
      AND
      ce.condition_era_start_date >= op.observation_period_start_date
      AND
      ce.condition_era_start_date <= op.observation_period_end_date
),
overallStats (
  stratum1_id, avg_value, stdev_value, min_value, max_value, total
) AS (
  SELECT
    stratum1_id,
    CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
    CAST(STDEV(count_value) AS FLOAT) AS stdev_value,
    MIN(count_value) AS min_value,
    MAX(count_value) AS max_value,
    COUNT_BIG(*) AS total
  FROM rawData
  GROUP BY stratum1_id
),
statsView (stratum1_id, count_value, total, rn) AS (
  SELECT
    stratum1_id,
    count_value,
    COUNT_BIG(*) AS total,
    ROW_NUMBER() OVER (PARTITION BY stratum1_id ORDER BY count_value) AS rn
  FROM rawData
  GROUP BY stratum1_id, count_value
),
priorStats (stratum1_id, count_value, total, accumulated) AS (
  SELECT
    s.stratum1_id,
    s.count_value,
    s.total,
    SUM(p.total) AS accumulated
  FROM statsView AS s
  INNER JOIN statsView AS p ON s.stratum1_id = p.stratum1_id AND p.rn <= s.rn
  GROUP BY s.stratum1_id, s.count_value, s.total, s.rn
)
SELECT
  1007 AS analysis_id,
  o.total AS count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(p.stratum1_id AS VARCHAR(255)) AS stratum_1,
  MIN(
    CASE
      WHEN p.accumulated >= .50 * o.total THEN count_value ELSE o.max_value
    END
  ) AS median_value,
  MIN(
    CASE
      WHEN p.accumulated >= .10 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p10_value,
  MIN(
    CASE
      WHEN p.accumulated >= .25 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p25_value,
  MIN(
    CASE
      WHEN p.accumulated >= .75 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p75_value,
  MIN(
    CASE
      WHEN p.accumulated >= .90 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p90_value
FROM priorStats AS p
INNER JOIN overallStats AS o ON p.stratum1_id = o.stratum1_id
GROUP BY
  p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
