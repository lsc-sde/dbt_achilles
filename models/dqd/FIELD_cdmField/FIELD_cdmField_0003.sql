WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if EPISODE_ID is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_id' as checkid
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
 WHEN COUNT(EPISODE_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_NUMBER is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_NUMBER' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_number' as checkid
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
 WHEN COUNT(EPISODE_NUMBER) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_OBJECT_CONCEPT_ID is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_OBJECT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_object_concept_id' as checkid
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
 WHEN COUNT(EPISODE_OBJECT_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_PARENT_ID is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_PARENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_parent_id' as checkid
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
 WHEN COUNT(EPISODE_PARENT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_SOURCE_CONCEPT_ID is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_source_concept_id' as checkid
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
 WHEN COUNT(EPISODE_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_SOURCE_VALUE is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_source_value' as checkid
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
 WHEN COUNT(EPISODE_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_START_DATE is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_start_date' as checkid
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
 WHEN COUNT(EPISODE_START_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_START_DATETIME is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_START_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_start_datetime' as checkid
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
 WHEN COUNT(EPISODE_START_DATETIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_TYPE_CONCEPT_ID is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_episode_type_concept_id' as checkid
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
 WHEN COUNT(EPISODE_TYPE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if PERSON_ID is present in the EPISODE table as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_person_id' as checkid
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
 FROM hive_metastore.dev_vc.EPISODE cdmTable
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
 ,'A yes or no value indicating if EPISODE_EVENT_FIELD_CONCEPT_ID is present in the EPISODE_EVENT table as expected based on the specification.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'EPISODE_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_event_episode_event_field_concept_id' as checkid
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
 WHEN COUNT(EPISODE_EVENT_FIELD_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
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
 ,'A yes or no value indicating if EPISODE_ID is present in the EPISODE_EVENT table as expected based on the specification.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'EPISODE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_event_episode_id' as checkid
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
 WHEN COUNT(EPISODE_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
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
 ,'A yes or no value indicating if EVENT_ID is present in the EPISODE_EVENT table as expected based on the specification.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'EVENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_episode_event_event_id' as checkid
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
 WHEN COUNT(EVENT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
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
 ,'A yes or no value indicating if DOMAIN_CONCEPT_ID_1 is present in the FACT_RELATIONSHIP table as expected based on the specification.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'DOMAIN_CONCEPT_ID_1' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_fact_relationship_domain_concept_id_1' as checkid
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
 WHEN COUNT(DOMAIN_CONCEPT_ID_1) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
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
 ,'A yes or no value indicating if DOMAIN_CONCEPT_ID_2 is present in the FACT_RELATIONSHIP table as expected based on the specification.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'DOMAIN_CONCEPT_ID_2' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_fact_relationship_domain_concept_id_2' as checkid
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
 WHEN COUNT(DOMAIN_CONCEPT_ID_2) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
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
 ,'A yes or no value indicating if FACT_ID_1 is present in the FACT_RELATIONSHIP table as expected based on the specification.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'FACT_ID_1' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_fact_relationship_fact_id_1' as checkid
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
 WHEN COUNT(FACT_ID_1) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
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
 ,'A yes or no value indicating if FACT_ID_2 is present in the FACT_RELATIONSHIP table as expected based on the specification.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'FACT_ID_2' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_fact_relationship_fact_id_2' as checkid
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
 WHEN COUNT(FACT_ID_2) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
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
 ,'A yes or no value indicating if RELATIONSHIP_CONCEPT_ID is present in the FACT_RELATIONSHIP table as expected based on the specification.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_fact_relationship_relationship_concept_id' as checkid
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
 WHEN COUNT(RELATIONSHIP_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
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
 ,'A yes or no value indicating if ADDRESS_1 is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'ADDRESS_1' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_address_1' as checkid
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
 WHEN COUNT(ADDRESS_1) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if ADDRESS_2 is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'ADDRESS_2' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_address_2' as checkid
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
 WHEN COUNT(ADDRESS_2) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if CITY is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'CITY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_city' as checkid
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
 WHEN COUNT(CITY) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if COUNTRY_CONCEPT_ID is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'COUNTRY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_country_concept_id' as checkid
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
 WHEN COUNT(COUNTRY_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if COUNTRY_SOURCE_VALUE is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'COUNTRY_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_country_source_value' as checkid
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
 WHEN COUNT(COUNTRY_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if COUNTY is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'COUNTY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_county' as checkid
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
 WHEN COUNT(COUNTY) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if LATITUDE is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'LATITUDE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_latitude' as checkid
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
 WHEN COUNT(LATITUDE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if LOCATION_ID is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'LOCATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_location_id' as checkid
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
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if LOCATION_SOURCE_VALUE is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'LOCATION_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_location_source_value' as checkid
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
 WHEN COUNT(LOCATION_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if LONGITUDE is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'LONGITUDE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_longitude' as checkid
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
 WHEN COUNT(LONGITUDE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if STATE is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'STATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_state' as checkid
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
 WHEN COUNT(STATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if ZIP is present in the LOCATION table as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'ZIP' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_location_zip' as checkid
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
 WHEN COUNT(ZIP) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.LOCATION cdmTable
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
 ,'A yes or no value indicating if MEAS_EVENT_FIELD_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEAS_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_meas_event_field_concept_id' as checkid
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
 WHEN COUNT(MEAS_EVENT_FIELD_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_concept_id' as checkid
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
 WHEN COUNT(MEASUREMENT_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_DATE is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_date' as checkid
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
 WHEN COUNT(MEASUREMENT_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_DATETIME is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_datetime' as checkid
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
 WHEN COUNT(MEASUREMENT_DATETIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_EVENT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_EVENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_event_id' as checkid
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
 WHEN COUNT(MEASUREMENT_EVENT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_id' as checkid
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
 WHEN COUNT(MEASUREMENT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_SOURCE_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_source_concept_id' as checkid
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
 WHEN COUNT(MEASUREMENT_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_SOURCE_VALUE is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_source_value' as checkid
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
 WHEN COUNT(MEASUREMENT_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_TIME is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_TIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_time' as checkid
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
 WHEN COUNT(MEASUREMENT_TIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if MEASUREMENT_TYPE_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_measurement_type_concept_id' as checkid
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
 WHEN COUNT(MEASUREMENT_TYPE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if OPERATOR_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'OPERATOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_operator_concept_id' as checkid
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
 WHEN COUNT(OPERATOR_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if PERSON_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_person_id' as checkid
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
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if PROVIDER_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_provider_id' as checkid
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
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if RANGE_HIGH is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'RANGE_HIGH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_range_high' as checkid
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
 WHEN COUNT(RANGE_HIGH) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if RANGE_LOW is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'RANGE_LOW' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_range_low' as checkid
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
 WHEN COUNT(RANGE_LOW) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if UNIT_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_unit_concept_id' as checkid
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
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if UNIT_SOURCE_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_unit_source_concept_id' as checkid
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
 WHEN COUNT(UNIT_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if UNIT_SOURCE_VALUE is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_unit_source_value' as checkid
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
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if VALUE_AS_CONCEPT_ID is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'VALUE_AS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_value_as_concept_id' as checkid
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
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
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
 ,'A yes or no value indicating if VALUE_AS_NUMBER is present in the MEASUREMENT table as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'VALUE_AS_NUMBER' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_measurement_value_as_number' as checkid
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
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte
)

SELECT *
FROM cte_all
