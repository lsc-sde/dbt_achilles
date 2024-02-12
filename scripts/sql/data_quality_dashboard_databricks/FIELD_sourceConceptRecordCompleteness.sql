WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field CONDITION_SOURCE_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_condition_occurrence_condition_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,10 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'CONDITION_OCCURRENCE.CONDITION_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field CAUSE_SOURCE_CONCEPT_ID in the DEATH table.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_death_cause_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,10 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'DEATH.CAUSE_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE cdmTable.CAUSE_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field DEVICE_SOURCE_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_device_exposure_device_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,10 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'DEVICE_EXPOSURE.DEVICE_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.DEVICE_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field UNIT_SOURCE_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_device_exposure_unit_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,10 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'DEVICE_EXPOSURE.UNIT_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.UNIT_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field DRUG_SOURCE_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'DRUG_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_drug_exposure_drug_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,10 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'DRUG_EXPOSURE.DRUG_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 WHERE cdmTable.DRUG_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field EPISODE_SOURCE_CONCEPT_ID in the EPISODE table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_episode_episode_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'EPISODE.EPISODE_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 WHERE cdmTable.EPISODE_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field MEASUREMENT_SOURCE_CONCEPT_ID in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_measurement_measurement_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'MEASUREMENT.MEASUREMENT_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 WHERE cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field UNIT_SOURCE_CONCEPT_ID in the MEASUREMENT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_measurement_unit_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 50 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 50 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,50 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'MEASUREMENT.UNIT_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 WHERE cdmTable.UNIT_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field OBSERVATION_SOURCE_CONCEPT_ID in the OBSERVATION table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_observation_observation_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'OBSERVATION.OBSERVATION_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 WHERE cdmTable.OBSERVATION_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field PROCEDURE_SOURCE_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_procedure_occurrence_procedure_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field SPECIALTY_SOURCE_CONCEPT_ID in the PROVIDER table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_provider_specialty_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'PROVIDER.SPECIALTY_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.SPECIALTY_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field VISIT_DETAIL_SOURCE_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_visit_detail_visit_detail_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'VISIT_DETAIL.VISIT_DETAIL_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 WHERE cdmTable.VISIT_DETAIL_SOURCE_CONCEPT_ID = 0 
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
 ,'sourceConceptRecordCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value of 0 in the source concept field VISIT_SOURCE_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_concept_record_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_sourceconceptrecordcompleteness_visit_occurrence_visit_source_concept_id' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'VISIT_OCCURRENCE.VISIT_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
 WHERE cdmTable.VISIT_SOURCE_CONCEPT_ID = 0 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_OCCURRENCE cdmTable
) denominator
) cte
)
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
