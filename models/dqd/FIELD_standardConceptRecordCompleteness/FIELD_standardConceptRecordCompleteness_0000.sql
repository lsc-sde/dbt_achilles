WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field PLACE_OF_SERVICE_CONCEPT_ID in the CARE_SITE table.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'PLACE_OF_SERVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_care_site_place_of_service_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'CARE_SITE.PLACE_OF_SERVICE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
 WHERE cdmTable.PLACE_OF_SERVICE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_CONCEPT_ID in the CONDITION_ERA table.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_condition_era_condition_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'CONDITION_ERA.CONDITION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE cdmTable.CONDITION_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_condition_occurrence_condition_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'CONDITION_OCCURRENCE.CONDITION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_STATUS_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_STATUS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_condition_occurrence_condition_status_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'CONDITION_OCCURRENCE.CONDITION_STATUS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_STATUS_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_TYPE_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_condition_occurrence_condition_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'CONDITION_OCCURRENCE.CONDITION_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field COST_TYPE_CONCEPT_ID in the COST table.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_cost_cost_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'COST.COST_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE cdmTable.COST_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.COST cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field CAUSE_CONCEPT_ID in the DEATH table.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_death_cause_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DEATH.CAUSE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE cdmTable.CAUSE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DEATH_TYPE_CONCEPT_ID in the DEATH table.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_death_death_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DEATH.DEATH_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE cdmTable.DEATH_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DEVICE_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_device_exposure_device_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DEVICE_EXPOSURE.DEVICE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.DEVICE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DEVICE_TYPE_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_device_exposure_device_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DEVICE_EXPOSURE.DEVICE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.DEVICE_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_device_exposure_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DEVICE_EXPOSURE.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.UNIT_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DRUG_CONCEPT_ID in the DOSE_ERA table.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_dose_era_drug_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DOSE_ERA.DRUG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.DRUG_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the DOSE_ERA table.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_dose_era_unit_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DOSE_ERA.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.UNIT_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DRUG_CONCEPT_ID in the DRUG_ERA table.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_drug_era_drug_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DRUG_ERA.DRUG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE cdmTable.DRUG_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DRUG_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_drug_exposure_drug_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DRUG_EXPOSURE.DRUG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 WHERE cdmTable.DRUG_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DRUG_TYPE_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'DRUG_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_drug_exposure_drug_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DRUG_EXPOSURE.DRUG_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 WHERE cdmTable.DRUG_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field ROUTE_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'ROUTE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_drug_exposure_route_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'DRUG_EXPOSURE.ROUTE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 WHERE cdmTable.ROUTE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field EPISODE_CONCEPT_ID in the EPISODE table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_episode_episode_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'EPISODE.EPISODE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 WHERE cdmTable.EPISODE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DOMAIN_CONCEPT_ID_1 in the FACT_RELATIONSHIP table.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'DOMAIN_CONCEPT_ID_1' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_fact_relationship_domain_concept_id_1' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_1' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
 WHERE cdmTable.DOMAIN_CONCEPT_ID_1 = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DOMAIN_CONCEPT_ID_2 in the FACT_RELATIONSHIP table.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'DOMAIN_CONCEPT_ID_2' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_fact_relationship_domain_concept_id_2' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_2' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
 WHERE cdmTable.DOMAIN_CONCEPT_ID_2 = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field RELATIONSHIP_CONCEPT_ID in the FACT_RELATIONSHIP table.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_fact_relationship_relationship_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
 WHERE cdmTable.RELATIONSHIP_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field COUNTRY_CONCEPT_ID in the LOCATION table.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'COUNTRY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_location_country_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'LOCATION.COUNTRY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.LOCATION cdmTable
 WHERE cdmTable.COUNTRY_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field MEASUREMENT_CONCEPT_ID in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_measurement_measurement_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'MEASUREMENT.MEASUREMENT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 WHERE cdmTable.MEASUREMENT_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field MEASUREMENT_TYPE_CONCEPT_ID in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_measurement_measurement_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'MEASUREMENT.MEASUREMENT_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 WHERE cdmTable.MEASUREMENT_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_measurement_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'MEASUREMENT.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 WHERE cdmTable.UNIT_CONCEPT_ID = 0 AND cdmTable.value_as_number IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 WHERE cdmTable.value_as_number IS NOT NULL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field OBSERVATION_CONCEPT_ID in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_observation_observation_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBSERVATION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE cdmTable.OBSERVATION_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field OBSERVATION_TYPE_CONCEPT_ID in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_observation_observation_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.OBSERVATION_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE cdmTable.OBSERVATION_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_observation_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE cdmTable.UNIT_CONCEPT_ID = 0 AND cdmTable.value_as_number IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE cdmTable.value_as_number IS NOT NULL
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field PERIOD_TYPE_CONCEPT_ID in the OBSERVATION_PERIOD table.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_observation_period_period_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'OBSERVATION_PERIOD.PERIOD_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 WHERE cdmTable.PERIOD_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field ETHNICITY_CONCEPT_ID in the PERSON table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_person_ethnicity_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.ETHNICITY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.ETHNICITY_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field GENDER_CONCEPT_ID in the PERSON table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_person_gender_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.GENDER_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field RACE_CONCEPT_ID in the PERSON table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_person_race_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.RACE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.RACE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field MODIFIER_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'MODIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_procedure_occurrence_modifier_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.MODIFIER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.MODIFIER_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field PROCEDURE_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_procedure_occurrence_procedure_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field PROCEDURE_TYPE_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_procedure_occurrence_procedure_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field GENDER_CONCEPT_ID in the PROVIDER table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_provider_gender_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.GENDER_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field SPECIALTY_CONCEPT_ID in the PROVIDER table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_provider_specialty_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.SPECIALTY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.SPECIALTY_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field SPECIMEN_CONCEPT_ID in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_specimen_specimen_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'SPECIMEN.SPECIMEN_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE cdmTable.SPECIMEN_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field SPECIMEN_TYPE_CONCEPT_ID in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_specimen_specimen_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'SPECIMEN.SPECIMEN_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE cdmTable.SPECIMEN_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the SPECIMEN table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_specimen_unit_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'SPECIMEN.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 WHERE cdmTable.UNIT_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field ADMITTED_FROM_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'ADMITTED_FROM_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_detail_admitted_from_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_DETAIL.ADMITTED_FROM_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE cdmTable.ADMITTED_FROM_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DISCHARGED_TO_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_detail_discharged_to_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_DETAIL.DISCHARGED_TO_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE cdmTable.DISCHARGED_TO_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field VISIT_DETAIL_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_detail_visit_detail_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_DETAIL.VISIT_DETAIL_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE cdmTable.VISIT_DETAIL_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field VISIT_DETAIL_TYPE_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_detail_visit_detail_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_DETAIL.VISIT_DETAIL_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field ADMITTED_FROM_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'ADMITTED_FROM_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_occurrence_admitted_from_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_OCCURRENCE.ADMITTED_FROM_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE cdmTable.ADMITTED_FROM_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field DISCHARGED_TO_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_occurrence_discharged_to_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_OCCURRENCE.DISCHARGED_TO_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE cdmTable.DISCHARGED_TO_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field VISIT_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_occurrence_visit_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_OCCURRENCE.VISIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE cdmTable.VISIT_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'standardConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the standard concept field VISIT_TYPE_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_standardconceptrecordcompleteness_visit_occurrence_visit_type_concept_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_OCCURRENCE.VISIT_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE cdmTable.VISIT_TYPE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
