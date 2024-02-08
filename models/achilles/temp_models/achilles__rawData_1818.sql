-- 1818	Number of observation records below/within/above normal range, by observation_concept_id and unit_concept_id
--HINT DISTRIBUTE_ON_KEY(person_id)
SELECT
  m.person_id,
  m.measurement_concept_id,
  m.unit_concept_id,
  CAST(CASE
    WHEN m.value_as_number < m.range_low
      THEN 'Below Range Low'
    WHEN m.value_as_number >= m.range_low AND m.value_as_number <= m.range_high
      THEN 'Within Range'
    WHEN m.value_as_number > m.range_high
      THEN 'Above Range High'
    ELSE 'Other'
  END AS VARCHAR(255)) AS stratum_3
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
WHERE
  m.value_as_number IS NOT NULL
  AND
  m.unit_concept_id IS NOT NULL
  AND
  m.range_low IS NOT NULL
  AND
  m.range_high IS NOT NULL
