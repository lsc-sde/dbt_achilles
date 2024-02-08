-- 717	Distribution of quantity by drug_concept_id
--HINT DISTRIBUTE_ON_KEY(stratum_id)
WITH rawData (stratum_id, count_value) AS (
  SELECT
    de.drug_concept_id AS stratum_id,
    CAST(de.quantity AS FLOAT) AS count_value
  FROM
    {{ source("omop", "drug_exposure" ) }} AS de
    JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      de.person_id = op.person_id
      AND
      de.drug_exposure_start_date >= op.observation_period_start_date
      AND
      de.drug_exposure_start_date <= op.observation_period_end_date
  WHERE
    de.quantity IS NOT NULL
),
overallStats (
  stratum_id, avg_value, stdev_value, min_value, max_value, total
) AS (
  SELECT
    stratum_id,
    CAST(avg(1.0 * count_value) AS FLOAT) AS avg_value,
    CAST(stddev(count_value) AS FLOAT) AS stdev_value,
    min(count_value) AS min_value,
    max(count_value) AS max_value,
    count(*) AS total
  FROM rawData
  GROUP BY stratum_id
),
statsView (stratum_id, count_value, total, rn) AS (
  SELECT
    stratum_id,
    count_value,
    count(*) AS total,
    ROW_NUMBER() OVER (ORDER BY count_value) AS rn
  FROM rawData
  GROUP BY stratum_id, count_value
),
priorStats (stratum_id, count_value, total, accumulated) AS (
  SELECT
    s.stratum_id,
    s.count_value,
    s.total,
    sum(p.total) AS accumulated
  FROM statsView AS s
  INNER JOIN statsView AS p ON s.stratum_id = p.stratum_id AND p.rn <= s.rn
  GROUP BY s.stratum_id, s.count_value, s.total, s.rn
)
SELECT
  717 AS analysis_id,
  o.total AS count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(o.stratum_id AS VARCHAR(255)) AS stratum_id,
  min(
    CASE
      WHEN p.accumulated >= .50 * o.total THEN count_value ELSE o.max_value
    END
  ) AS median_value,
  min(
    CASE
      WHEN p.accumulated >= .10 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p10_value,
  min(
    CASE
      WHEN p.accumulated >= .25 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p25_value,
  min(
    CASE
      WHEN p.accumulated >= .75 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p75_value,
  min(
    CASE
      WHEN p.accumulated >= .90 * o.total THEN count_value ELSE o.max_value
    END
  ) AS p90_value
FROM priorStats AS p
INNER JOIN overallStats AS o ON p.stratum_id = o.stratum_id
GROUP BY
  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
