WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PAYER_SOURCE_VALUE of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_payer_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PAYER_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.PAYER_SOURCE_VALUE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_person_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.PERSON_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PLAN_CONCEPT_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_plan_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PLAN_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.PLAN_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PLAN_SOURCE_CONCEPT_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_plan_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PLAN_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.PLAN_SOURCE_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PLAN_SOURCE_VALUE of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_plan_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.PLAN_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.PLAN_SOURCE_VALUE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SPONSOR_CONCEPT_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_sponsor_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.SPONSOR_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.SPONSOR_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SPONSOR_SOURCE_CONCEPT_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_sponsor_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.SPONSOR_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.SPONSOR_SOURCE_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SPONSOR_SOURCE_VALUE of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_sponsor_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.SPONSOR_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.SPONSOR_SOURCE_VALUE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the STOP_REASON_CONCEPT_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_stop_reason_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.STOP_REASON_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.STOP_REASON_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the STOP_REASON_SOURCE_CONCEPT_ID of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_stop_reason_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.STOP_REASON_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.STOP_REASON_SOURCE_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the STOP_REASON_SOURCE_VALUE of the PAYER_PLAN_PERIOD.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_payer_plan_period_stop_reason_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PAYER_PLAN_PERIOD.STOP_REASON_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.STOP_REASON_SOURCE_VALUE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the BIRTH_DATETIME of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'BIRTH_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_birth_datetime' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.BIRTH_DATETIME' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.BIRTH_DATETIME IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CARE_SITE_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_care_site_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.CARE_SITE_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DAY_OF_BIRTH of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'DAY_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_day_of_birth' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.DAY_OF_BIRTH' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.DAY_OF_BIRTH IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the ETHNICITY_CONCEPT_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_ethnicity_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.ETHNICITY_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the ETHNICITY_SOURCE_CONCEPT_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_ethnicity_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.ETHNICITY_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.ETHNICITY_SOURCE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the ETHNICITY_SOURCE_VALUE of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_ethnicity_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.ETHNICITY_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.ETHNICITY_SOURCE_VALUE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the GENDER_CONCEPT_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_gender_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.GENDER_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the GENDER_SOURCE_CONCEPT_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_gender_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.GENDER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.GENDER_SOURCE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the GENDER_SOURCE_VALUE of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_gender_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.GENDER_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.GENDER_SOURCE_VALUE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the LOCATION_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'LOCATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_location_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.LOCATION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.LOCATION_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the MONTH_OF_BIRTH of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'MONTH_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_month_of_birth' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.MONTH_OF_BIRTH' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.MONTH_OF_BIRTH IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_person_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_SOURCE_VALUE of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PERSON_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_person_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.PERSON_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.PERSON_SOURCE_VALUE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROVIDER_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_provider_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.PROVIDER_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the RACE_CONCEPT_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_race_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.RACE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the RACE_SOURCE_CONCEPT_ID of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_race_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.RACE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.RACE_SOURCE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the RACE_SOURCE_VALUE of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_race_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PERSON.RACE_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 WHERE cdmTable.RACE_SOURCE_VALUE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the YEAR_OF_BIRTH of the PERSON.' as check_description
 ,'PERSON' as cdm_table_name
 ,'YEAR_OF_BIRTH' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_person_year_of_birth' as checkid
 ,0 as is_error
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
 WHERE cdmTable.YEAR_OF_BIRTH IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the MODIFIER_CONCEPT_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'MODIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_modifier_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.MODIFIER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.MODIFIER_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the MODIFIER_SOURCE_VALUE of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'MODIFIER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_modifier_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.MODIFIER_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.MODIFIER_SOURCE_VALUE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_person_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_CONCEPT_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_DATE of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_date' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_DATE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_DATETIME of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_datetime' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_DATETIME' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_DATETIME IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_END_DATE of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_end_date' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_END_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_END_DATE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_END_DATETIME of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_END_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_end_datetime' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_END_DATETIME' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_END_DATETIME IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_OCCURRENCE_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_occurrence_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.PROCEDURE_OCCURRENCE_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_SOURCE_CONCEPT_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_SOURCE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_SOURCE_VALUE of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROCEDURE_SOURCE_VALUE IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROCEDURE_TYPE_CONCEPT_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_procedure_type_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.PROCEDURE_TYPE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PROVIDER_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_provider_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.PROVIDER_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the QUANTITY of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'QUANTITY' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_quantity' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.QUANTITY' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.QUANTITY IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the VISIT_DETAIL_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_visit_detail_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.VISIT_DETAIL_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the VISIT_OCCURRENCE_ID of the PROCEDURE_OCCURRENCE.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_procedure_occurrence_visit_occurrence_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROCEDURE_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE cdmTable.VISIT_OCCURRENCE_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CARE_SITE_ID of the PROVIDER.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_provider_care_site_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.CARE_SITE_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEA of the PROVIDER.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'DEA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_provider_dea' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.DEA' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.DEA IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the GENDER_CONCEPT_ID of the PROVIDER.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_provider_gender_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.GENDER_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the GENDER_SOURCE_CONCEPT_ID of the PROVIDER.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_provider_gender_source_concept_id' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.GENDER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.GENDER_SOURCE_CONCEPT_ID IS NULL
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
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the GENDER_SOURCE_VALUE of the PROVIDER.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_provider_gender_source_value' as checkid
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'PROVIDER.GENDER_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 WHERE cdmTable.GENDER_SOURCE_VALUE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
