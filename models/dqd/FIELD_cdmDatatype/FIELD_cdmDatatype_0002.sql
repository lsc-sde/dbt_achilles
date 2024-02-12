WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the ENCODING_CONCEPT_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'ENCODING_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_encoding_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.ENCODING_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.ENCODING_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.ENCODING_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.ENCODING_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.ENCODING_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the LANGUAGE_CONCEPT_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'LANGUAGE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_language_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.LANGUAGE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.LANGUAGE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.LANGUAGE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.LANGUAGE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.LANGUAGE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_CLASS_CONCEPT_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_CLASS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_note_class_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.NOTE_CLASS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_CLASS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_CLASS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_CLASS_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_CLASS_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_EVENT_FIELD_CONCEPT_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_note_event_field_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.NOTE_EVENT_FIELD_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_note_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.NOTE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_TYPE_CONCEPT_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_note_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.NOTE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_person_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_provider_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_visit_detail_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the NOTE is the expected data type based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_visit_occurrence_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_ID in the NOTE_NLP is the expected data type based on the specification.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_nlp_note_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE_NLP.NOTE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_NLP_CONCEPT_ID in the NOTE_NLP is the expected data type based on the specification.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_NLP_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_nlp_note_nlp_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE_NLP.NOTE_NLP_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_NLP_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_NLP_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_NLP_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_NLP_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_NLP_ID in the NOTE_NLP is the expected data type based on the specification.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_NLP_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_nlp_note_nlp_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE_NLP.NOTE_NLP_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_NLP_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_NLP_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_NLP_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_NLP_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the NOTE_NLP_SOURCE_CONCEPT_ID in the NOTE_NLP is the expected data type based on the specification.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_NLP_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_nlp_note_nlp_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE_NLP.NOTE_NLP_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SECTION_CONCEPT_ID in the NOTE_NLP is the expected data type based on the specification.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'SECTION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_note_nlp_section_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'NOTE_NLP.SECTION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SECTION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SECTION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SECTION_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SECTION_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the OBS_EVENT_FIELD_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBS_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_obs_event_field_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBS_EVENT_FIELD_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.OBS_EVENT_FIELD_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.OBS_EVENT_FIELD_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.OBS_EVENT_FIELD_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.OBS_EVENT_FIELD_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the OBSERVATION_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_observation_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBSERVATION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.OBSERVATION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.OBSERVATION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.OBSERVATION_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.OBSERVATION_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the OBSERVATION_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_observation_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBSERVATION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.OBSERVATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.OBSERVATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.OBSERVATION_ID) AS STRING),'.') != 0))
 AND cdmTable.OBSERVATION_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the OBSERVATION_SOURCE_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_observation_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBSERVATION_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.OBSERVATION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.OBSERVATION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.OBSERVATION_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.OBSERVATION_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the OBSERVATION_TYPE_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_observation_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBSERVATION_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.OBSERVATION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.OBSERVATION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.OBSERVATION_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.OBSERVATION_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_person_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_provider_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the QUALIFIER_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'QUALIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_qualifier_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.QUALIFIER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.QUALIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.QUALIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.QUALIFIER_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.QUALIFIER_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_visit_detail_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the OBSERVATION is the expected data type based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_visit_occurrence_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the OBSERVATION_PERIOD_ID in the OBSERVATION_PERIOD is the expected data type based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'OBSERVATION_PERIOD_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_period_observation_period_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION_PERIOD.OBSERVATION_PERIOD_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.OBSERVATION_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.OBSERVATION_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.OBSERVATION_PERIOD_ID) AS STRING),'.') != 0))
 AND cdmTable.OBSERVATION_PERIOD_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERIOD_TYPE_CONCEPT_ID in the OBSERVATION_PERIOD is the expected data type based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_period_period_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION_PERIOD.PERIOD_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERIOD_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERIOD_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERIOD_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PERIOD_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the OBSERVATION_PERIOD is the expected data type based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_observation_period_person_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION_PERIOD.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PAYER_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_payer_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PAYER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PAYER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PAYER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PAYER_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PAYER_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PAYER_PLAN_PERIOD_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_payer_plan_period_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PAYER_PLAN_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PAYER_PLAN_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PAYER_PLAN_PERIOD_ID) AS STRING),'.') != 0))
 AND cdmTable.PAYER_PLAN_PERIOD_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PAYER_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_payer_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PAYER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PAYER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PAYER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PAYER_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PAYER_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_person_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PLAN_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_plan_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PLAN_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PLAN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PLAN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PLAN_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PLAN_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PLAN_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_plan_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PLAN_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PLAN_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PLAN_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PLAN_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PLAN_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPONSOR_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_sponsor_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.SPONSOR_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPONSOR_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPONSOR_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPONSOR_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SPONSOR_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPONSOR_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_sponsor_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.SPONSOR_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPONSOR_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPONSOR_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPONSOR_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SPONSOR_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the STOP_REASON_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_stop_reason_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.STOP_REASON_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.STOP_REASON_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.STOP_REASON_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.STOP_REASON_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.STOP_REASON_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the STOP_REASON_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_payer_plan_period_stop_reason_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.STOP_REASON_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.STOP_REASON_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.STOP_REASON_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.STOP_REASON_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.STOP_REASON_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CARE_SITE_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_care_site_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
 AND cdmTable.CARE_SITE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DAY_OF_BIRTH in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'DAY_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_day_of_birth' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.DAY_OF_BIRTH' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DAY_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DAY_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DAY_OF_BIRTH) AS STRING),'.') != 0))
 AND cdmTable.DAY_OF_BIRTH IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the ETHNICITY_CONCEPT_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_ethnicity_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.ETHNICITY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.ETHNICITY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.ETHNICITY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.ETHNICITY_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.ETHNICITY_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the ETHNICITY_SOURCE_CONCEPT_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_ethnicity_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.ETHNICITY_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.ETHNICITY_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.ETHNICITY_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.ETHNICITY_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.ETHNICITY_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the GENDER_CONCEPT_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_gender_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.GENDER_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.GENDER_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the GENDER_SOURCE_CONCEPT_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_gender_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.GENDER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.GENDER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.GENDER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.GENDER_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.GENDER_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the LOCATION_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'LOCATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_location_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.LOCATION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.LOCATION_ID) AS STRING),'.') != 0))
 AND cdmTable.LOCATION_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the MONTH_OF_BIRTH in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'MONTH_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_month_of_birth' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.MONTH_OF_BIRTH' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.MONTH_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.MONTH_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.MONTH_OF_BIRTH) AS STRING),'.') != 0))
 AND cdmTable.MONTH_OF_BIRTH IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_person_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_provider_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the RACE_CONCEPT_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_race_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,0 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.RACE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.RACE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.RACE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.RACE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.RACE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON
) denominator
) cte
)

SELECT *
FROM cte_all
