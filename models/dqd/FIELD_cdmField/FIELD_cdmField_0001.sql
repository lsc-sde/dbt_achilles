WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmField' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if AMOUNT_ALLOWED is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'AMOUNT_ALLOWED' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_amount_allowed' as checkid
 ,0 as is_error
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
 WHEN COUNT(AMOUNT_ALLOWED) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if COST_DOMAIN_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_DOMAIN_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_cost_domain_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(COST_DOMAIN_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if COST_EVENT_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_EVENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_cost_event_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(COST_EVENT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if COST_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_cost_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(COST_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if COST_TYPE_CONCEPT_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_cost_type_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(COST_TYPE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if CURRENCY_CONCEPT_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'CURRENCY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_currency_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(CURRENCY_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if DRG_CONCEPT_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'DRG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_drg_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DRG_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if DRG_SOURCE_VALUE is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'DRG_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_drg_source_value' as checkid
 ,0 as is_error
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
 WHEN COUNT(DRG_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_BY_PATIENT is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_BY_PATIENT' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_by_patient' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_BY_PATIENT) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_BY_PAYER is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_BY_PAYER' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_by_payer' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_BY_PAYER) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_BY_PRIMARY is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_BY_PRIMARY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_by_primary' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_BY_PRIMARY) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_DISPENSING_FEE is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_DISPENSING_FEE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_dispensing_fee' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_DISPENSING_FEE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_INGREDIENT_COST is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_INGREDIENT_COST' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_ingredient_cost' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_INGREDIENT_COST) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_PATIENT_COINSURANCE is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_PATIENT_COINSURANCE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_patient_coinsurance' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_PATIENT_COINSURANCE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_PATIENT_COPAY is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_PATIENT_COPAY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_patient_copay' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_PATIENT_COPAY) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAID_PATIENT_DEDUCTIBLE is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAID_PATIENT_DEDUCTIBLE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_paid_patient_deductible' as checkid
 ,0 as is_error
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
 WHEN COUNT(PAID_PATIENT_DEDUCTIBLE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if PAYER_PLAN_PERIOD_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_payer_plan_period_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if REVENUE_CODE_CONCEPT_ID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'REVENUE_CODE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_revenue_code_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(REVENUE_CODE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if REVENUE_CODE_SOURCE_VALUE is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'REVENUE_CODE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_revenue_code_source_value' as checkid
 ,0 as is_error
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
 WHEN COUNT(REVENUE_CODE_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if TOTAL_CHARGE is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'TOTAL_CHARGE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_total_charge' as checkid
 ,0 as is_error
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
 WHEN COUNT(TOTAL_CHARGE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if TOTAL_COST is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'TOTAL_COST' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_total_cost' as checkid
 ,0 as is_error
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
 WHEN COUNT(TOTAL_COST) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if TOTAL_PAID is present in the COST table as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'TOTAL_PAID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_cost_total_paid' as checkid
 ,0 as is_error
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
 WHEN COUNT(TOTAL_PAID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.COST cdmTable
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
 ,'A yes or no value indicating if CAUSE_CONCEPT_ID is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_cause_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(CAUSE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if CAUSE_SOURCE_CONCEPT_ID is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_cause_source_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(CAUSE_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if CAUSE_SOURCE_VALUE is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'CAUSE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_cause_source_value' as checkid
 ,0 as is_error
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
 WHEN COUNT(CAUSE_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if DEATH_DATE is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_death_date' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEATH_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if DEATH_DATETIME is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_death_datetime' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEATH_DATETIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if DEATH_TYPE_CONCEPT_ID is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_death_type_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEATH_TYPE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if PERSON_ID is present in the DEATH table as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_death_person_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEATH cdmTable
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
 ,'A yes or no value indicating if DEVICE_CONCEPT_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_EXPOSURE_END_DATE is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_exposure_end_date' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_EXPOSURE_END_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_EXPOSURE_END_DATETIME is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_END_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_exposure_end_datetime' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_EXPOSURE_END_DATETIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_EXPOSURE_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_exposure_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_EXPOSURE_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_EXPOSURE_START_DATE is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_exposure_start_date' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_EXPOSURE_START_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_EXPOSURE_START_DATETIME is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_START_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_exposure_start_datetime' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_EXPOSURE_START_DATETIME) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_SOURCE_CONCEPT_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_source_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_SOURCE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_SOURCE_VALUE is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_source_value' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_SOURCE_VALUE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DEVICE_TYPE_CONCEPT_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_device_type_concept_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DEVICE_TYPE_CONCEPT_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if PERSON_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_person_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if PRODUCTION_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'PRODUCTION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_production_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(PRODUCTION_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if PROVIDER_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_provider_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if QUANTITY is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'QUANTITY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_quantity' as checkid
 ,0 as is_error
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
 WHEN COUNT(QUANTITY) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if UNIQUE_DEVICE_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIQUE_DEVICE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_unique_device_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(UNIQUE_DEVICE_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if UNIT_CONCEPT_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_unit_concept_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if UNIT_SOURCE_CONCEPT_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_unit_source_concept_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if UNIT_SOURCE_VALUE is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'UNIT_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_unit_source_value' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if VISIT_DETAIL_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_visit_detail_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if VISIT_OCCURRENCE_ID is present in the DEVICE_EXPOSURE table as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_device_exposure_visit_occurrence_id' as checkid
 ,0 as is_error
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
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
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
 ,'A yes or no value indicating if DOSE_ERA_END_DATE is present in the DOSE_ERA table as expected based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_dose_era_dose_era_end_date' as checkid
 ,0 as is_error
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
 WHEN COUNT(DOSE_ERA_END_DATE) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
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
 ,'A yes or no value indicating if DOSE_ERA_ID is present in the DOSE_ERA table as expected based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_field.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmfield_dose_era_dose_era_id' as checkid
 ,0 as is_error
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
 WHEN COUNT(DOSE_ERA_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte
)

SELECT *
FROM cte_all
