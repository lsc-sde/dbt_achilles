WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PERSON_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_person_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PERSON_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PROVIDER_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_provider_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PROVIDER_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if QUALIFIER_CONCEPT_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'QUALIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_qualifier_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(QUALIFIER_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if QUALIFIER_SOURCE_VALUE is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'QUALIFIER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_qualifier_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(QUALIFIER_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if UNIT_CONCEPT_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_unit_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(UNIT_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if UNIT_SOURCE_VALUE is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'UNIT_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_unit_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(UNIT_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if VALUE_AS_CONCEPT_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VALUE_AS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_value_as_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(VALUE_AS_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if VALUE_AS_NUMBER is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VALUE_AS_NUMBER' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_value_as_number' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(VALUE_AS_NUMBER) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if VALUE_AS_STRING is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VALUE_AS_STRING' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_value_as_string' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(VALUE_AS_STRING) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if VALUE_SOURCE_VALUE is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VALUE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_value_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(VALUE_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if VISIT_DETAIL_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_visit_detail_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(VISIT_DETAIL_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if VISIT_OCCURRENCE_ID is present in the OBSERVATION table as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_visit_occurrence_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(VISIT_OCCURRENCE_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if OBSERVATION_PERIOD_END_DATE is present in the OBSERVATION_PERIOD table as expected based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'OBSERVATION_PERIOD_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_period_observation_period_end_date' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(OBSERVATION_PERIOD_END_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if OBSERVATION_PERIOD_ID is present in the OBSERVATION_PERIOD table as expected based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'OBSERVATION_PERIOD_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_period_observation_period_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(OBSERVATION_PERIOD_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if OBSERVATION_PERIOD_START_DATE is present in the OBSERVATION_PERIOD table as expected based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'OBSERVATION_PERIOD_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_period_observation_period_start_date' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(OBSERVATION_PERIOD_START_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PERIOD_TYPE_CONCEPT_ID is present in the OBSERVATION_PERIOD table as expected based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_period_period_type_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PERIOD_TYPE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PERSON_ID is present in the OBSERVATION_PERIOD table as expected based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_observation_period_person_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PERSON_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if FAMILY_SOURCE_VALUE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'FAMILY_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_family_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(FAMILY_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PAYER_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_payer_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PAYER_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PAYER_PLAN_PERIOD_END_DATE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_PLAN_PERIOD_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_payer_plan_period_end_date' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PAYER_PLAN_PERIOD_END_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PAYER_PLAN_PERIOD_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_payer_plan_period_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PAYER_PLAN_PERIOD_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PAYER_PLAN_PERIOD_START_DATE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_PLAN_PERIOD_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_payer_plan_period_start_date' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PAYER_PLAN_PERIOD_START_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PAYER_SOURCE_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_payer_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PAYER_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PAYER_SOURCE_VALUE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_payer_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PAYER_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PERSON_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_person_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PERSON_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PLAN_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_plan_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PLAN_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PLAN_SOURCE_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_plan_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PLAN_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PLAN_SOURCE_VALUE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_plan_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PLAN_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if SPONSOR_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_sponsor_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(SPONSOR_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if SPONSOR_SOURCE_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_sponsor_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(SPONSOR_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if SPONSOR_SOURCE_VALUE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_sponsor_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(SPONSOR_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if STOP_REASON_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_stop_reason_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(STOP_REASON_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if STOP_REASON_SOURCE_CONCEPT_ID is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_stop_reason_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(STOP_REASON_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if STOP_REASON_SOURCE_VALUE is present in the PAYER_PLAN_PERIOD table as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_payer_plan_period_stop_reason_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(STOP_REASON_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if BIRTH_DATETIME is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'BIRTH_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_birth_datetime' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(BIRTH_DATETIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if CARE_SITE_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_care_site_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(CARE_SITE_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if DAY_OF_BIRTH is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'DAY_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_day_of_birth' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(DAY_OF_BIRTH) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if ETHNICITY_CONCEPT_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_ethnicity_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(ETHNICITY_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if ETHNICITY_SOURCE_CONCEPT_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_ethnicity_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(ETHNICITY_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if ETHNICITY_SOURCE_VALUE is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_ethnicity_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(ETHNICITY_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if GENDER_CONCEPT_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_gender_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(GENDER_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if GENDER_SOURCE_CONCEPT_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_gender_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(GENDER_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if GENDER_SOURCE_VALUE is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_gender_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(GENDER_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if LOCATION_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'LOCATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_location_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(LOCATION_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if MONTH_OF_BIRTH is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'MONTH_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_month_of_birth' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(MONTH_OF_BIRTH) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PERSON_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_person_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PERSON_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PERSON_SOURCE_VALUE is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PERSON_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_person_source_value' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PERSON_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if PROVIDER_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_provider_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(PROVIDER_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if RACE_CONCEPT_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_race_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(RACE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if RACE_SOURCE_CONCEPT_ID is present in the PERSON table as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_person_race_source_concept_id' as checkid
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE
 WHEN COUNT(RACE_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte
)

SELECT *
FROM cte_all
