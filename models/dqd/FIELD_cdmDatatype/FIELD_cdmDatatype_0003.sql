WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the RACE_SOURCE_CONCEPT_ID in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_race_source_concept_id' as checkid
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
 'PERSON.RACE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.RACE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.RACE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.RACE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.RACE_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'A yes or no value indicating if the YEAR_OF_BIRTH in the PERSON is the expected data type based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'YEAR_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_person_year_of_birth' as checkid
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
 'PERSON.YEAR_OF_BIRTH' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.YEAR_OF_BIRTH) AS STRING),'.') != 0))
 AND cdmTable.YEAR_OF_BIRTH IS NOT NULL
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
 ,'A yes or no value indicating if the MODIFIER_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'MODIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_modifier_concept_id' as checkid
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
 'PROCEDURE_OCCURRENCE.MODIFIER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.MODIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.MODIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.MODIFIER_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.MODIFIER_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_person_id' as checkid
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
 'PROCEDURE_OCCURRENCE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROCEDURE_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_procedure_concept_id' as checkid
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROCEDURE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROCEDURE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROCEDURE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PROCEDURE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROCEDURE_OCCURRENCE_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_procedure_occurrence_id' as checkid
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROCEDURE_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROCEDURE_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROCEDURE_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.PROCEDURE_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROCEDURE_SOURCE_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_procedure_source_concept_id' as checkid
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROCEDURE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROCEDURE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROCEDURE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PROCEDURE_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROCEDURE_TYPE_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_procedure_type_concept_id' as checkid
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROCEDURE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROCEDURE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROCEDURE_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PROCEDURE_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_provider_id' as checkid
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
 'PROCEDURE_OCCURRENCE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the QUANTITY in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'QUANTITY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_quantity' as checkid
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
 'PROCEDURE_OCCURRENCE.QUANTITY' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.QUANTITY) AS STRING),'.') != 0))
 AND cdmTable.QUANTITY IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_visit_detail_id' as checkid
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
 'PROCEDURE_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_procedure_occurrence_visit_occurrence_id' as checkid
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
 'PROCEDURE_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CARE_SITE_ID in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_care_site_id' as checkid
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
 'PROVIDER.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
 AND cdmTable.CARE_SITE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the GENDER_CONCEPT_ID in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_gender_concept_id' as checkid
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
 'PROVIDER.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.GENDER_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.GENDER_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the GENDER_SOURCE_CONCEPT_ID in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_gender_source_concept_id' as checkid
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
 'PROVIDER.GENDER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.GENDER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.GENDER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.GENDER_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.GENDER_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_provider_id' as checkid
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
 'PROVIDER.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPECIALTY_CONCEPT_ID in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_specialty_concept_id' as checkid
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
 'PROVIDER.SPECIALTY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPECIALTY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPECIALTY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPECIALTY_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SPECIALTY_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPECIALTY_SOURCE_CONCEPT_ID in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_specialty_source_concept_id' as checkid
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
 'PROVIDER.SPECIALTY_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPECIALTY_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPECIALTY_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPECIALTY_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SPECIALTY_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the YEAR_OF_BIRTH in the PROVIDER is the expected data type based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'YEAR_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_provider_year_of_birth' as checkid
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
 'PROVIDER.YEAR_OF_BIRTH' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.YEAR_OF_BIRTH) AS STRING),'.') != 0))
 AND cdmTable.YEAR_OF_BIRTH IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SOURCE_CONCEPT_ID in the SOURCE_TO_CONCEPT_MAP is the expected data type based on the specification.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_source_to_concept_map_source_concept_id' as checkid
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
 'SOURCE_TO_CONCEPT_MAP.SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the TARGET_CONCEPT_ID in the SOURCE_TO_CONCEPT_MAP is the expected data type based on the specification.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'TARGET_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_source_to_concept_map_target_concept_id' as checkid
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
 'SOURCE_TO_CONCEPT_MAP.TARGET_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.TARGET_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.TARGET_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.TARGET_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.TARGET_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the ANATOMIC_SITE_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'ANATOMIC_SITE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_anatomic_site_concept_id' as checkid
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
 'SPECIMEN.ANATOMIC_SITE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.ANATOMIC_SITE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.ANATOMIC_SITE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.ANATOMIC_SITE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.ANATOMIC_SITE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DISEASE_STATUS_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'DISEASE_STATUS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_disease_status_concept_id' as checkid
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
 'SPECIMEN.DISEASE_STATUS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DISEASE_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DISEASE_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DISEASE_STATUS_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DISEASE_STATUS_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_person_id' as checkid
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
 'SPECIMEN.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPECIMEN_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_specimen_concept_id' as checkid
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
 'SPECIMEN.SPECIMEN_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPECIMEN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPECIMEN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPECIMEN_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SPECIMEN_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPECIMEN_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_specimen_id' as checkid
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
 'SPECIMEN.SPECIMEN_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPECIMEN_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPECIMEN_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPECIMEN_ID) AS STRING),'.') != 0))
 AND cdmTable.SPECIMEN_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SPECIMEN_TYPE_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_specimen_type_concept_id' as checkid
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
 'SPECIMEN.SPECIMEN_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SPECIMEN_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SPECIMEN_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SPECIMEN_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SPECIMEN_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_specimen_unit_concept_id' as checkid
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
 'SPECIMEN.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CARE_SITE_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_care_site_id' as checkid
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
 'VISIT_DETAIL.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
 AND cdmTable.CARE_SITE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DISCHARGED_TO_CONCEPT_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_discharged_to_concept_id' as checkid
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
 'VISIT_DETAIL.DISCHARGED_TO_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DISCHARGED_TO_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DISCHARGED_TO_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PARENT_VISIT_DETAIL_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PARENT_VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_parent_visit_detail_id' as checkid
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
 'VISIT_DETAIL.PARENT_VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PARENT_VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PARENT_VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PARENT_VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.PARENT_VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_person_id' as checkid
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
 'VISIT_DETAIL.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PRECEDING_VISIT_DETAIL_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PRECEDING_VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_preceding_visit_detail_id' as checkid
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
 'VISIT_DETAIL.PRECEDING_VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PRECEDING_VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PRECEDING_VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PRECEDING_VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.PRECEDING_VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_provider_id' as checkid
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
 'VISIT_DETAIL.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_CONCEPT_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_visit_detail_concept_id' as checkid
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
 'VISIT_DETAIL.VISIT_DETAIL_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_visit_detail_id' as checkid
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
 'VISIT_DETAIL.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_TYPE_CONCEPT_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_visit_detail_type_concept_id' as checkid
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
 'VISIT_DETAIL.VISIT_DETAIL_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the VISIT_DETAIL is the expected data type based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_detail_visit_occurrence_id' as checkid
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
 'VISIT_DETAIL.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the ADMITTED_FROM_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'ADMITTED_FROM_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_admitted_from_concept_id' as checkid
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
 'VISIT_OCCURRENCE.ADMITTED_FROM_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.ADMITTED_FROM_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.ADMITTED_FROM_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.ADMITTED_FROM_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.ADMITTED_FROM_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CARE_SITE_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_care_site_id' as checkid
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
 'VISIT_OCCURRENCE.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
 AND cdmTable.CARE_SITE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DISCHARGED_TO_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_discharged_to_concept_id' as checkid
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
 'VISIT_OCCURRENCE.DISCHARGED_TO_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DISCHARGED_TO_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DISCHARGED_TO_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_person_id' as checkid
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
 'VISIT_OCCURRENCE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PRECEDING_VISIT_OCCURRENCE_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'PRECEDING_VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_preceding_visit_occurrence_id' as checkid
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
 'VISIT_OCCURRENCE.PRECEDING_VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PRECEDING_VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PRECEDING_VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PRECEDING_VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.PRECEDING_VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_provider_id' as checkid
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
 'VISIT_OCCURRENCE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_visit_concept_id' as checkid
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
 'VISIT_OCCURRENCE.VISIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_visit_occurrence_id' as checkid
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
 'VISIT_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_SOURCE_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_visit_source_concept_id' as checkid
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
 'VISIT_OCCURRENCE.VISIT_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE
) denominator
) cte
)

SELECT *
FROM cte_all
