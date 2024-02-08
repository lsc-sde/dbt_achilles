-- 1100	Number of persons by location 3-digit zip
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    LEFT(l1.zip, 3) AS stratum_1,
    COUNT_BIG(DISTINCT person_id) AS count_value
  FROM {{ ref(  var("achilles_source_schema") + "__person" ) }} AS p1
  INNER JOIN {{ ref(  var("achilles_source_schema") + "__location" ) }} AS l1
    ON p1.location_id = l1.location_id
  WHERE
    p1.location_id IS NOT NULL
    AND l1.zip IS NOT NULL
  GROUP BY LEFT(l1.zip, 3)
)
SELECT
  1100 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(null AS VARCHAR(255)) AS stratum_2,
  CAST(null AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5
FROM rawData
