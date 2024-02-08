-- 1326	Number of records by domain by visit detail concept id
SELECT
  1326 AS analysis_id,
  v.cdm_table AS stratum_2,
  v.record_count AS count_value,
  CAST(v.visit_detail_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5
FROM (
  SELECT
    'drug_exposure' AS cdm_table,
    COALESCE(vd.visit_detail_concept_id, 0) AS visit_detail_concept_id,
    COUNT(*) AS record_count
  FROM
    {{ ref(  var("achilles_source_schema") + "__drug_exposure" ) }} AS de
  LEFT JOIN
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    ON
      de.visit_occurrence_id = vd.visit_occurrence_id
  GROUP BY
    vd.visit_detail_concept_id
  UNION
  SELECT
    'condition_occurrence' AS cdm_table,
    COALESCE(vd.visit_detail_concept_id, 0) AS visit_detail_concept_id,
    COUNT(*) AS record_count
  FROM
    {{ ref(  var("achilles_source_schema") + "__condition_occurrence" ) }} AS co
  LEFT JOIN
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    ON
      co.visit_occurrence_id = vd.visit_occurrence_id
  GROUP BY
    vd.visit_detail_concept_id
  UNION
  SELECT
    'device_exposure' AS cdm_table,
    COALESCE(visit_detail_concept_id, 0) AS visit_detail_concept_id,
    COUNT(*) AS record_count
  FROM
    {{ ref(  var("achilles_source_schema") + "__device_exposure" ) }} AS de
  LEFT JOIN
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    ON
      de.visit_occurrence_id = vd.visit_occurrence_id
  GROUP BY
    vd.visit_detail_concept_id
  UNION
  SELECT
    'procedure_occurrence' AS cdm_table,
    COALESCE(vd.visit_detail_concept_id, 0) AS visit_detail_concept_id,
    COUNT(*) AS record_count
  FROM
    {{ ref(  var("achilles_source_schema") + "__procedure_occurrence" ) }} AS po
  LEFT JOIN
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    ON
      po.visit_occurrence_id = vd.visit_occurrence_id
  GROUP BY
    vd.visit_detail_concept_id
  UNION
  SELECT
    'measurement' AS cdm_table,
    COALESCE(vd.visit_detail_concept_id, 0) AS visit_detail_concept_id,
    COUNT(*) AS record_count
  FROM
    {{ ref(  var("achilles_source_schema") + "__measurement" ) }} AS m
  LEFT JOIN
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    ON
      m.visit_occurrence_id = vd.visit_occurrence_id
  GROUP BY
    vd.visit_detail_concept_id
  UNION
  SELECT
    'observation' AS cdm_table,
    COALESCE(vd.visit_detail_concept_id, 0) AS visit_detail_concept_id,
    COUNT(*) AS record_count
  FROM
    {{ ref(  var("achilles_source_schema") + "__observation" ) }} AS o
  LEFT JOIN
    {{ ref(  var("achilles_source_schema") + "__visit_detail" ) }} AS vd
    ON
      o.visit_occurrence_id = vd.visit_occurrence_id
  GROUP BY
    vd.visit_detail_concept_id
) AS v
