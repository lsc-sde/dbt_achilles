WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CARE_SITE_ID in the CARE_SITE is the expected data type based on the specification.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_care_site_care_site_id' as checkid
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
 'CARE_SITE.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
 AND cdmTable.CARE_SITE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CARE_SITE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the LOCATION_ID in the CARE_SITE is the expected data type based on the specification.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'LOCATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_care_site_location_id' as checkid
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
 'CARE_SITE.LOCATION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.LOCATION_ID) AS STRING),'.') != 0))
 AND cdmTable.LOCATION_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CARE_SITE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PLACE_OF_SERVICE_CONCEPT_ID in the CARE_SITE is the expected data type based on the specification.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'PLACE_OF_SERVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_care_site_place_of_service_concept_id' as checkid
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
 'CARE_SITE.PLACE_OF_SERVICE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PLACE_OF_SERVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PLACE_OF_SERVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PLACE_OF_SERVICE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.PLACE_OF_SERVICE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CARE_SITE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CDM_VERSION_CONCEPT_ID in the CDM_SOURCE is the expected data type based on the specification.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_VERSION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cdm_source_cdm_version_concept_id' as checkid
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
 'CDM_SOURCE.CDM_VERSION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CDM_VERSION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CDM_VERSION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CDM_VERSION_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CDM_VERSION_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CDM_SOURCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the COHORT_DEFINITION_ID in the COHORT is the expected data type based on the specification.' as check_description
 ,'COHORT' as cdm_table_name
 ,'COHORT_DEFINITION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cohort_cohort_definition_id' as checkid
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
 'COHORT.COHORT_DEFINITION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.COHORT_DEFINITION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.COHORT_DEFINITION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.COHORT_DEFINITION_ID) AS STRING),'.') != 0))
 AND cdmTable.COHORT_DEFINITION_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SUBJECT_ID in the COHORT is the expected data type based on the specification.' as check_description
 ,'COHORT' as cdm_table_name
 ,'SUBJECT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cohort_subject_id' as checkid
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
 'COHORT.SUBJECT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SUBJECT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SUBJECT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SUBJECT_ID) AS STRING),'.') != 0))
 AND cdmTable.SUBJECT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the COHORT_DEFINITION_ID in the COHORT_DEFINITION is the expected data type based on the specification.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'COHORT_DEFINITION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cohort_definition_cohort_definition_id' as checkid
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
 'COHORT_DEFINITION.COHORT_DEFINITION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.COHORT_DEFINITION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.COHORT_DEFINITION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.COHORT_DEFINITION_ID) AS STRING),'.') != 0))
 AND cdmTable.COHORT_DEFINITION_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DEFINITION_TYPE_CONCEPT_ID in the COHORT_DEFINITION is the expected data type based on the specification.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'DEFINITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cohort_definition_definition_type_concept_id' as checkid
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
 'COHORT_DEFINITION.DEFINITION_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DEFINITION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DEFINITION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DEFINITION_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DEFINITION_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the SUBJECT_CONCEPT_ID in the COHORT_DEFINITION is the expected data type based on the specification.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'SUBJECT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cohort_definition_subject_concept_id' as checkid
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
 'COHORT_DEFINITION.SUBJECT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.SUBJECT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.SUBJECT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.SUBJECT_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.SUBJECT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_CONCEPT_ID in the CONDITION_ERA is the expected data type based on the specification.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_era_condition_concept_id' as checkid
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
 'CONDITION_ERA.CONDITION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_ERA_ID in the CONDITION_ERA is the expected data type based on the specification.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_era_condition_era_id' as checkid
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
 'CONDITION_ERA.CONDITION_ERA_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_ERA_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_ERA_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_OCCURRENCE_COUNT in the CONDITION_ERA is the expected data type based on the specification.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_OCCURRENCE_COUNT' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_era_condition_occurrence_count' as checkid
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
 'CONDITION_ERA.CONDITION_OCCURRENCE_COUNT' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_OCCURRENCE_COUNT AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_OCCURRENCE_COUNT AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_OCCURRENCE_COUNT) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_OCCURRENCE_COUNT IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the CONDITION_ERA is the expected data type based on the specification.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_era_person_id' as checkid
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
 'CONDITION_ERA.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_condition_concept_id' as checkid
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
 'CONDITION_OCCURRENCE.CONDITION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_OCCURRENCE_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_condition_occurrence_id' as checkid
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
 'CONDITION_OCCURRENCE.CONDITION_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_SOURCE_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_condition_source_concept_id' as checkid
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
 'CONDITION_OCCURRENCE.CONDITION_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_STATUS_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_STATUS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_condition_status_concept_id' as checkid
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
 'CONDITION_OCCURRENCE.CONDITION_STATUS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_STATUS_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_STATUS_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CONDITION_TYPE_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_condition_type_concept_id' as checkid
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
 'CONDITION_OCCURRENCE.CONDITION_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CONDITION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CONDITION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CONDITION_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CONDITION_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_person_id' as checkid
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
 'CONDITION_OCCURRENCE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_provider_id' as checkid
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
 'CONDITION_OCCURRENCE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_visit_detail_id' as checkid
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
 'CONDITION_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_condition_occurrence_visit_occurrence_id' as checkid
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
 'CONDITION_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the COST_EVENT_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_EVENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_cost_event_id' as checkid
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
 'COST.COST_EVENT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.COST_EVENT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.COST_EVENT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.COST_EVENT_ID) AS STRING),'.') != 0))
 AND cdmTable.COST_EVENT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the COST_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_cost_id' as checkid
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
 'COST.COST_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.COST_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.COST_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.COST_ID) AS STRING),'.') != 0))
 AND cdmTable.COST_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the COST_TYPE_CONCEPT_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_cost_type_concept_id' as checkid
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
 'COST.COST_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.COST_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.COST_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.COST_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.COST_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CURRENCY_CONCEPT_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'CURRENCY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_currency_concept_id' as checkid
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
 'COST.CURRENCY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CURRENCY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CURRENCY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CURRENCY_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CURRENCY_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DRG_CONCEPT_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'DRG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_drg_concept_id' as checkid
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
 'COST.DRG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DRG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DRG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DRG_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DRG_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PAYER_PLAN_PERIOD_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_payer_plan_period_id' as checkid
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
 'COST.PAYER_PLAN_PERIOD_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PAYER_PLAN_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PAYER_PLAN_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PAYER_PLAN_PERIOD_ID) AS STRING),'.') != 0))
 AND cdmTable.PAYER_PLAN_PERIOD_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the REVENUE_CODE_CONCEPT_ID in the COST is the expected data type based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'REVENUE_CODE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_cost_revenue_code_concept_id' as checkid
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
 'COST.REVENUE_CODE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.REVENUE_CODE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.REVENUE_CODE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.REVENUE_CODE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.REVENUE_CODE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CAUSE_CONCEPT_ID in the DEATH is the expected data type based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_death_cause_concept_id' as checkid
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
 'DEATH.CAUSE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CAUSE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CAUSE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CAUSE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CAUSE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the CAUSE_SOURCE_CONCEPT_ID in the DEATH is the expected data type based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_death_cause_source_concept_id' as checkid
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
 'DEATH.CAUSE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.CAUSE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.CAUSE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.CAUSE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.CAUSE_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DEATH_TYPE_CONCEPT_ID in the DEATH is the expected data type based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_death_death_type_concept_id' as checkid
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
 'DEATH.DEATH_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DEATH_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DEATH_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DEATH_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DEATH_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the DEATH is the expected data type based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_death_person_id' as checkid
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
 'DEATH.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DEVICE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_device_concept_id' as checkid
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
 'DEVICE_EXPOSURE.DEVICE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DEVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DEVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DEVICE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DEVICE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DEVICE_EXPOSURE_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_device_exposure_id' as checkid
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
 'DEVICE_EXPOSURE.DEVICE_EXPOSURE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DEVICE_EXPOSURE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DEVICE_EXPOSURE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DEVICE_EXPOSURE_ID) AS STRING),'.') != 0))
 AND cdmTable.DEVICE_EXPOSURE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DEVICE_SOURCE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_device_source_concept_id' as checkid
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
 'DEVICE_EXPOSURE.DEVICE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DEVICE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DEVICE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DEVICE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DEVICE_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DEVICE_TYPE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_device_type_concept_id' as checkid
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
 'DEVICE_EXPOSURE.DEVICE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DEVICE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DEVICE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DEVICE_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DEVICE_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_person_id' as checkid
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
 'DEVICE_EXPOSURE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PROVIDER_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_provider_id' as checkid
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
 'DEVICE_EXPOSURE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the QUANTITY in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'QUANTITY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_quantity' as checkid
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
 'DEVICE_EXPOSURE.QUANTITY' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.QUANTITY) AS STRING),'.') != 0))
 AND cdmTable.QUANTITY IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_unit_concept_id' as checkid
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
 'DEVICE_EXPOSURE.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the UNIT_SOURCE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_unit_source_concept_id' as checkid
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
 'DEVICE_EXPOSURE.UNIT_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.UNIT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.UNIT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.UNIT_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.UNIT_SOURCE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_DETAIL_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_visit_detail_id' as checkid
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
 'DEVICE_EXPOSURE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the DEVICE_EXPOSURE is the expected data type based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_device_exposure_visit_occurrence_id' as checkid
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
 'DEVICE_EXPOSURE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DOSE_ERA_ID in the DOSE_ERA is the expected data type based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_dose_era_dose_era_id' as checkid
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
 'DOSE_ERA.DOSE_ERA_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DOSE_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DOSE_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DOSE_ERA_ID) AS STRING),'.') != 0))
 AND cdmTable.DOSE_ERA_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DRUG_CONCEPT_ID in the DOSE_ERA is the expected data type based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_dose_era_drug_concept_id' as checkid
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
 'DOSE_ERA.DRUG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DRUG_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DRUG_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the PERSON_ID in the DOSE_ERA is the expected data type based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_dose_era_person_id' as checkid
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
 'DOSE_ERA.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
 AND cdmTable.PERSON_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the DOSE_ERA is the expected data type based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_dose_era_unit_concept_id' as checkid
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
 'DOSE_ERA.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DRUG_CONCEPT_ID in the DRUG_ERA is the expected data type based on the specification.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_drug_era_drug_concept_id' as checkid
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
 'DRUG_ERA.DRUG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DRUG_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DRUG_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_ERA
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DRUG_ERA_ID in the DRUG_ERA is the expected data type based on the specification.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_drug_era_drug_era_id' as checkid
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
 'DRUG_ERA.DRUG_ERA_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DRUG_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DRUG_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DRUG_ERA_ID) AS STRING),'.') != 0))
 AND cdmTable.DRUG_ERA_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_ERA
) denominator
) cte
)

SELECT *
FROM cte_all
