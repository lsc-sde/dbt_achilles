-- 206	Distribution of age by visit_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum1_id)
WITH rawData (stratum1_id, stratum2_id, count_value) AS (
  SELECT
    v.visit_concept_id AS stratum1_id,
    p.gender_concept_id AS stratum2_id,
    v.visit_start_year - p.year_of_birth AS count_value
  FROM
    {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p
    JOIN (
    SELECT
      vo.person_id,
      vo.visit_concept_id,
      MIN(YEAR(vo.visit_start_date)) AS visit_start_year
    FROM
      {{ ref(  var("achilles_source_schema") + "__visit_occurrence" ) }} AS vo
    INNER JOIN
      {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
      ON
        vo.person_id = op.person_id
        AND
        vo.visit_start_date >= op.observation_period_start_date
        AND
        vo.visit_start_date <= op.observation_period_end_date
    GROUP BY
      vo.person_id,
      vo.visit_concept_id
  ) AS v
    ON
      p.person_id = v.person_id
),
overallStats (
  stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total
) AS (
  SELECT
    stratum1_id,
    stratum2_id,
    CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
    CAST(STDEV(count_value) AS FLOAT) AS stdev_value,
    MIN(count_value) AS min_value,
    MAX(count_value) AS max_value,
    COUNT_BIG(*) AS total
  FROM rawData
  GROUP BY stratum1_id, stratum2_id
),
statsView (stratum1_id, stratum2_id, count_value, total, rn) AS (
  SELECT
    stratum1_id,
    stratum2_id,
    count_value,
    COUNT_BIG(*) AS total,
    ROW_NUMBER() OVER (
      PARTITION BY stratum1_id, stratum2_id ORDER BY count_value
    ) AS rn
  FROM rawData
  GROUP BY stratum1_id, stratum2_id, count_value
),
priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) AS (
  SELECT
    s.stratum1_id,
    s.stratum2_id,
    s.count_value,
    s.total,
    SUM(p.total) AS accumulated
  FROM statsView AS s
  INNER JOIN
    statsView AS p
    ON
      s.stratum1_id = p.stratum1_id
      AND s.stratum2_id = p.stratum2_id
      AND p.rn <= s.rn
  GROUP BY s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
SELECT
  206 AS analysis_id,
  o.total AS count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(o.stratum1_id AS VARCHAR(255)) AS stratum1_id,
  CAST(o.stratum2_id AS VARCHAR(255)) AS stratum2_id,
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
INNER JOIN
  overallStats AS o
  ON p.stratum1_id = o.stratum1_id AND p.stratum2_id = o.stratum2_id
GROUP BY
  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
