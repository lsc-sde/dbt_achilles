-- 512	Distribution of time from death to last drug
--HINT DISTRIBUTE_ON_KEY(count_value)
WITH rawData (count_value) AS (
  SELECT datediff( de.max_date, d.death_date) AS count_value
  FROM
    {{ source("omop", "death" ) }} AS d
    JOIN (
    SELECT
      de.person_id,
      MAX(de.drug_exposure_start_date) AS max_date
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
    GROUP BY
      de.person_id
  ) AS de
    ON
      d.person_id = de.person_id
),
overallStats (avg_value, stdev_value, min_value, max_value, total) AS (
  SELECT
    CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
    CAST(stddev(count_value) AS FLOAT) AS stdev_value,
    MIN(count_value) AS min_value,
    MAX(count_value) AS max_value,
    count(*) AS total
  FROM rawData
),
statsView (count_value, total, rn) AS (
  SELECT
    count_value,
    count(*) AS total,
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
  512 AS analysis_id,
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
