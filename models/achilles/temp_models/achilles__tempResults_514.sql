-- 514	Distribution of time from death to last procedure
--HINT DISTRIBUTE_ON_KEY(count_value)
WITH rawData (count_value) AS (
  SELECT DATEDIFF(DD, d.death_date, po.max_date) AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__death" ) }} AS d
    JOIN (
    SELECT
      po.person_id,
      MAX(po.procedure_date) AS max_date
    FROM
      {{ ref(  var("achilles_source_schema") + "__procedure_occurrence" ) }} AS po
    INNER JOIN
      {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
      ON
        po.person_id = op.person_id
        AND
        po.procedure_date >= op.observation_period_start_date
        AND
        po.procedure_date <= op.observation_period_end_date
    GROUP BY
      po.person_id
  ) AS po
    ON
      d.person_id = po.person_id
),
overallStats (avg_value, stdev_value, min_value, max_value, total) AS (
  SELECT
    CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
    CAST(STDEV(count_value) AS FLOAT) AS stdev_value,
    MIN(count_value) AS min_value,
    MAX(count_value) AS max_value,
    COUNT_BIG(*) AS total
  FROM rawData
),
statsView (count_value, total, rn) AS (
  SELECT
    count_value,
    COUNT_BIG(*) AS total,
    ROW_NUMBER() OVER (ORDER BY count_value) AS rn
  FROM rawData
  GROUP BY count_value
),
priorStats (count_value, total, accumulated) AS (
  SELECT
    s.count_value,
    s.total,
    SUM(p.total) AS accumulated
  FROM statsView AS s
  INNER JOIN statsView AS p ON p.rn <= s.rn
  GROUP BY s.count_value, s.total, s.rn
)
SELECT
  514 AS analysis_id,
  o.total AS count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
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
CROSS JOIN overallStats AS o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
