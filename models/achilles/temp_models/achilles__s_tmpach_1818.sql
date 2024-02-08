--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  1818 AS analysis_id,
  CAST(measurement_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(unit_concept_id AS VARCHAR(255)) AS stratum_2,
  CAST(stratum_3 AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  COUNT_BIG(person_id) AS count_value
FROM
	achilles__rawData_1818
GROUP BY
	measurement_concept_id,
	unit_concept_id,
  stratum_3
