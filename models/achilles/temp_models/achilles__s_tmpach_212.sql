-- 212	Number of persons with at least one visit occurrence by calendar year by gender by age decile
--HINT DISTRIBUTE_ON_KEY(stratum_1)
WITH rawData AS (
  SELECT
    p1.gender_concept_id AS stratum_2,
    YEAR(visit_start_date) AS stratum_1,
    FLOOR((YEAR(visit_start_date) - p1.year_of_birth) / 10) AS stratum_3,
    COUNT_BIG(DISTINCT p1.PERSON_ID) AS count_value
  FROM {{ source("omop", "person" ) }} AS p1
  INNER JOIN
    {{ source("omop", "visit_occurrence" ) }} AS vo1
    ON p1.person_id = vo1.person_id
  GROUP BY
    YEAR(visit_start_date),
    p1.gender_concept_id,
    FLOOR((YEAR(visit_start_date) - p1.year_of_birth) / 10)
)
SELECT
  212 AS analysis_id,
  count_value,
  CAST(stratum_1 AS VARCHAR(255)) AS stratum_1,
  CAST(stratum_2 AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(null AS VARCHAR(255)) AS stratum_4,
  CAST(null AS VARCHAR(255)) AS stratum_5
FROM rawData
