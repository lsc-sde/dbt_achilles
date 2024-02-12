WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the PLACE_OF_SERVICE_CONCEPT_ID field in the CARE_SITE table.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'PLACE_OF_SERVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_care_site_place_of_service_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.PLACE_OF_SERVICE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the CDM_VERSION_CONCEPT_ID field in the CDM_SOURCE table.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_VERSION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_cdm_source_cdm_version_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.CDM_VERSION_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DEFINITION_TYPE_CONCEPT_ID field in the COHORT_DEFINITION table.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'DEFINITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_cohort_definition_definition_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DEFINITION_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the SUBJECT_CONCEPT_ID field in the COHORT_DEFINITION table.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'SUBJECT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_cohort_definition_subject_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.SUBJECT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the CONDITION_CONCEPT_ID field in the CONDITION_ERA table.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_condition_era_condition_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.CONDITION_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the CONDITION_CONCEPT_ID field in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_condition_occurrence_condition_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.CONDITION_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the CONDITION_STATUS_CONCEPT_ID field in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_STATUS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_condition_occurrence_condition_status_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.CONDITION_STATUS_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the CONDITION_TYPE_CONCEPT_ID field in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_condition_occurrence_condition_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.CONDITION_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the COST_TYPE_CONCEPT_ID field in the COST table.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_cost_cost_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.COST_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the CAUSE_CONCEPT_ID field in the DEATH table.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_death_cause_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.CAUSE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DEATH_TYPE_CONCEPT_ID field in the DEATH table.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_death_death_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DEATH_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DEVICE_CONCEPT_ID field in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_device_exposure_device_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DEVICE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DEVICE_TYPE_CONCEPT_ID field in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_device_exposure_device_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DEVICE_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the UNIT_CONCEPT_ID field in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_device_exposure_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DRUG_CONCEPT_ID field in the DOSE_ERA table.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_dose_era_drug_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DRUG_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the UNIT_CONCEPT_ID field in the DOSE_ERA table.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_dose_era_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DRUG_CONCEPT_ID field in the DRUG_ERA table.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_drug_era_drug_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DRUG_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DRUG_CONCEPT_ID field in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_drug_exposure_drug_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'DRUG_EXPOSURE.DRUG_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DRUG_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DRUG_TYPE_CONCEPT_ID field in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'DRUG_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_drug_exposure_drug_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'DRUG_EXPOSURE.DRUG_TYPE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DRUG_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the ROUTE_CONCEPT_ID field in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'ROUTE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_drug_exposure_route_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'DRUG_EXPOSURE.ROUTE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.ROUTE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the EPISODE_CONCEPT_ID field in the EPISODE table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_episode_episode_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'EPISODE.EPISODE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.EPISODE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the EPISODE_TYPE_CONCEPT_ID field in the EPISODE table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_episode_episode_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'EPISODE.EPISODE_TYPE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.EPISODE_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the EPISODE_EVENT_FIELD_CONCEPT_ID field in the EPISODE_EVENT table.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'EPISODE_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_episode_event_episode_event_field_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'EPISODE_EVENT.EPISODE_EVENT_FIELD_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.EPISODE_EVENT_FIELD_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the COUNTRY_CONCEPT_ID field in the LOCATION table.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'COUNTRY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_location_country_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'LOCATION.COUNTRY_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.LOCATION cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.COUNTRY_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the MEAS_EVENT_FIELD_CONCEPT_ID field in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEAS_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_measurement_meas_event_field_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'MEASUREMENT.MEAS_EVENT_FIELD_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.MEAS_EVENT_FIELD_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the MEASUREMENT_CONCEPT_ID field in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_measurement_measurement_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'MEASUREMENT.MEASUREMENT_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.MEASUREMENT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the MEASUREMENT_TYPE_CONCEPT_ID field in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_measurement_measurement_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'MEASUREMENT.MEASUREMENT_TYPE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.MEASUREMENT_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the OPERATOR_CONCEPT_ID field in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'OPERATOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_measurement_operator_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'MEASUREMENT.OPERATOR_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.OPERATOR_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the UNIT_CONCEPT_ID field in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_measurement_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'MEASUREMENT.UNIT_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the ENCODING_CONCEPT_ID field in the NOTE table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'ENCODING_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_encoding_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.ENCODING_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the LANGUAGE_CONCEPT_ID field in the NOTE table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'LANGUAGE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_language_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.LANGUAGE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the NOTE_CLASS_CONCEPT_ID field in the NOTE table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_CLASS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_note_class_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.NOTE_CLASS_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the NOTE_EVENT_FIELD_CONCEPT_ID field in the NOTE table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_note_event_field_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the NOTE_TYPE_CONCEPT_ID field in the NOTE table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_note_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.NOTE_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the NOTE_NLP_CONCEPT_ID field in the NOTE_NLP table.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_NLP_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_nlp_note_nlp_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.NOTE_NLP_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the SECTION_CONCEPT_ID field in the NOTE_NLP table.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'SECTION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_note_nlp_section_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.SECTION_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the OBS_EVENT_FIELD_CONCEPT_ID field in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBS_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_observation_obs_event_field_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.OBS_EVENT_FIELD_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the OBSERVATION_CONCEPT_ID field in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_observation_observation_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.OBSERVATION_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the OBSERVATION_TYPE_CONCEPT_ID field in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_observation_observation_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.OBSERVATION_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the QUALIFIER_CONCEPT_ID field in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'QUALIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_observation_qualifier_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.QUALIFIER_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the UNIT_CONCEPT_ID field in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_observation_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the PERIOD_TYPE_CONCEPT_ID field in the OBSERVATION_PERIOD table.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_observation_period_period_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.PERIOD_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the ETHNICITY_CONCEPT_ID field in the PERSON table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_person_ethnicity_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.ETHNICITY_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the GENDER_CONCEPT_ID field in the PERSON table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_person_gender_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.GENDER_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the RACE_CONCEPT_ID field in the PERSON table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_person_race_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.RACE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the MODIFIER_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'MODIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_procedure_occurrence_modifier_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.MODIFIER_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the PROCEDURE_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_procedure_occurrence_procedure_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.PROCEDURE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the PROCEDURE_TYPE_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_procedure_occurrence_procedure_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.PROCEDURE_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the GENDER_CONCEPT_ID field in the PROVIDER table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_provider_gender_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.GENDER_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the SPECIALTY_CONCEPT_ID field in the PROVIDER table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_provider_specialty_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.SPECIALTY_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
) denominator
) cte
)
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the TARGET_CONCEPT_ID field in the SOURCE_TO_CONCEPT_MAP table.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'TARGET_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_source_to_concept_map_target_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.TARGET_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the ANATOMIC_SITE_CONCEPT_ID field in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'ANATOMIC_SITE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_specimen_anatomic_site_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.ANATOMIC_SITE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DISEASE_STATUS_CONCEPT_ID field in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'DISEASE_STATUS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_specimen_disease_status_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DISEASE_STATUS_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the SPECIMEN_CONCEPT_ID field in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_specimen_specimen_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.SPECIMEN_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the SPECIMEN_TYPE_CONCEPT_ID field in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_specimen_specimen_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.SPECIMEN_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the UNIT_CONCEPT_ID field in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_specimen_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the ADMITTED_FROM_CONCEPT_ID field in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'ADMITTED_FROM_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_detail_admitted_from_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'VISIT_DETAIL.ADMITTED_FROM_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.ADMITTED_FROM_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DISCHARGED_TO_CONCEPT_ID field in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_detail_discharged_to_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DISCHARGED_TO_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the VISIT_DETAIL_CONCEPT_ID field in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_detail_visit_detail_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.VISIT_DETAIL_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the VISIT_DETAIL_TYPE_CONCEPT_ID field in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_detail_visit_detail_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the ADMITTED_FROM_CONCEPT_ID field in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'ADMITTED_FROM_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_occurrence_admitted_from_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.ADMITTED_FROM_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the DISCHARGED_TO_CONCEPT_ID field in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_occurrence_discharged_to_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.DISCHARGED_TO_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the VISIT_CONCEPT_ID field in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_occurrence_visit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.VISIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the VISIT_TYPE_CONCEPT_ID field in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_occurrence_visit_type_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,0 as threshold_value
 ,NULL as notes_value
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
 'VISIT_OCCURRENCE.VISIT_TYPE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 JOIN hive_metastore.dev_vc.concept co 
 ON cdmTable.VISIT_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte
)
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
