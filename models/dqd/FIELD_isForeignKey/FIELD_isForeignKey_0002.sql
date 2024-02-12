WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_OCCURRENCE_ID field in the OBSERVATION table that does not exist in the VISIT_OCCURRENCE table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_visit_occurrence_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_OCCURRENCE fkTable
 ON cdmTable.VISIT_OCCURRENCE_ID = fkTable.VISIT_OCCURRENCE_ID
 WHERE fkTable.VISIT_OCCURRENCE_ID IS NULL
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERIOD_TYPE_CONCEPT_ID field in the OBSERVATION_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_period_period_type_concept_id' as checkid
 ,0 as is_error
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
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PERIOD_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PERIOD_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the OBSERVATION_PERIOD table that does not exist in the PERSON table.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_period_person_id' as checkid
 ,0 as is_error
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
 'OBSERVATION_PERIOD.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PAYER_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_payer_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.PAYER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PAYER_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PAYER_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PAYER_SOURCE_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PAYER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_payer_source_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.PAYER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PAYER_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PAYER_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the PAYER_PLAN_PERIOD table that does not exist in the PERSON table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_person_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PLAN_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_plan_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.PLAN_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PLAN_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PLAN_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PLAN_SOURCE_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'PLAN_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_plan_source_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.PLAN_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PLAN_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PLAN_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SPONSOR_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_sponsor_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.SPONSOR_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SPONSOR_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SPONSOR_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SPONSOR_SOURCE_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'SPONSOR_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_sponsor_source_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.SPONSOR_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SPONSOR_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SPONSOR_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the STOP_REASON_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_stop_reason_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.STOP_REASON_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.STOP_REASON_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.STOP_REASON_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the STOP_REASON_SOURCE_CONCEPT_ID field in the PAYER_PLAN_PERIOD table that does not exist in the CONCEPT table.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'STOP_REASON_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_payer_plan_period_stop_reason_source_concept_id' as checkid
 ,0 as is_error
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
 'PAYER_PLAN_PERIOD.STOP_REASON_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PAYER_PLAN_PERIOD cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.STOP_REASON_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.STOP_REASON_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the CARE_SITE_ID field in the PERSON table that does not exist in the CARE_SITE table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_care_site_id' as checkid
 ,0 as is_error
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
 'PERSON.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CARE_SITE fkTable
 ON cdmTable.CARE_SITE_ID = fkTable.CARE_SITE_ID
 WHERE fkTable.CARE_SITE_ID IS NULL
 AND cdmTable.CARE_SITE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the ETHNICITY_CONCEPT_ID field in the PERSON table that does not exist in the CONCEPT table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_ethnicity_concept_id' as checkid
 ,0 as is_error
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
 'PERSON.ETHNICITY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.ETHNICITY_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.ETHNICITY_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the ETHNICITY_SOURCE_CONCEPT_ID field in the PERSON table that does not exist in the CONCEPT table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'ETHNICITY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_ethnicity_source_concept_id' as checkid
 ,0 as is_error
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
 'PERSON.ETHNICITY_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.ETHNICITY_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.ETHNICITY_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the GENDER_CONCEPT_ID field in the PERSON table that does not exist in the CONCEPT table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_gender_concept_id' as checkid
 ,0 as is_error
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
 'PERSON.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.GENDER_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.GENDER_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the GENDER_SOURCE_CONCEPT_ID field in the PERSON table that does not exist in the CONCEPT table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_gender_source_concept_id' as checkid
 ,0 as is_error
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
 'PERSON.GENDER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.GENDER_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.GENDER_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the LOCATION_ID field in the PERSON table that does not exist in the LOCATION table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'LOCATION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_location_id' as checkid
 ,0 as is_error
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
 'PERSON.LOCATION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.LOCATION fkTable
 ON cdmTable.LOCATION_ID = fkTable.LOCATION_ID
 WHERE fkTable.LOCATION_ID IS NULL
 AND cdmTable.LOCATION_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the PERSON table that does not exist in the PROVIDER table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_provider_id' as checkid
 ,0 as is_error
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
 'PERSON.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PROVIDER fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the RACE_CONCEPT_ID field in the PERSON table that does not exist in the CONCEPT table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_race_concept_id' as checkid
 ,0 as is_error
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
 'PERSON.RACE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.RACE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.RACE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the RACE_SOURCE_CONCEPT_ID field in the PERSON table that does not exist in the CONCEPT table.' as check_description
 ,'PERSON' as cdm_table_name
 ,'RACE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_person_race_source_concept_id' as checkid
 ,0 as is_error
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
 'PERSON.RACE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PERSON cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.RACE_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.RACE_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the MODIFIER_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the CONCEPT table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'MODIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_modifier_concept_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.MODIFIER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.MODIFIER_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.MODIFIER_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the PERSON table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_person_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROCEDURE_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the CONCEPT table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_procedure_concept_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PROCEDURE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PROCEDURE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROCEDURE_SOURCE_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the CONCEPT table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_procedure_source_concept_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PROCEDURE_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PROCEDURE_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROCEDURE_TYPE_CONCEPT_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the CONCEPT table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_procedure_type_concept_id' as checkid
 ,0 as is_error
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
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.PROCEDURE_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.PROCEDURE_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the PROVIDER table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_provider_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PROVIDER fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_DETAIL_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the VISIT_DETAIL table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_visit_detail_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_DETAIL fkTable
 ON cdmTable.VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_OCCURRENCE_ID field in the PROCEDURE_OCCURRENCE table that does not exist in the VISIT_OCCURRENCE table.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_procedure_occurrence_visit_occurrence_id' as checkid
 ,0 as is_error
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
 'PROCEDURE_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_OCCURRENCE fkTable
 ON cdmTable.VISIT_OCCURRENCE_ID = fkTable.VISIT_OCCURRENCE_ID
 WHERE fkTable.VISIT_OCCURRENCE_ID IS NULL
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the CARE_SITE_ID field in the PROVIDER table that does not exist in the CARE_SITE table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_provider_care_site_id' as checkid
 ,0 as is_error
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
 'PROVIDER.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CARE_SITE fkTable
 ON cdmTable.CARE_SITE_ID = fkTable.CARE_SITE_ID
 WHERE fkTable.CARE_SITE_ID IS NULL
 AND cdmTable.CARE_SITE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the GENDER_CONCEPT_ID field in the PROVIDER table that does not exist in the CONCEPT table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_provider_gender_concept_id' as checkid
 ,0 as is_error
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
 'PROVIDER.GENDER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.GENDER_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.GENDER_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the GENDER_SOURCE_CONCEPT_ID field in the PROVIDER table that does not exist in the CONCEPT table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_provider_gender_source_concept_id' as checkid
 ,0 as is_error
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
 'PROVIDER.GENDER_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.GENDER_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.GENDER_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SPECIALTY_CONCEPT_ID field in the PROVIDER table that does not exist in the CONCEPT table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_provider_specialty_concept_id' as checkid
 ,0 as is_error
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
 'PROVIDER.SPECIALTY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SPECIALTY_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SPECIALTY_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SPECIALTY_SOURCE_CONCEPT_ID field in the PROVIDER table that does not exist in the CONCEPT table.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'SPECIALTY_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_provider_specialty_source_concept_id' as checkid
 ,0 as is_error
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
 'PROVIDER.SPECIALTY_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.PROVIDER cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SPECIALTY_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SPECIALTY_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SOURCE_CONCEPT_ID field in the SOURCE_TO_CONCEPT_MAP table that does not exist in the CONCEPT table.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_source_to_concept_map_source_concept_id' as checkid
 ,0 as is_error
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
 'SOURCE_TO_CONCEPT_MAP.SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the TARGET_CONCEPT_ID field in the SOURCE_TO_CONCEPT_MAP table that does not exist in the CONCEPT table.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'TARGET_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_source_to_concept_map_target_concept_id' as checkid
 ,0 as is_error
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
 'SOURCE_TO_CONCEPT_MAP.TARGET_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.TARGET_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.TARGET_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the TARGET_VOCABULARY_ID field in the SOURCE_TO_CONCEPT_MAP table that does not exist in the VOCABULARY table.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'TARGET_VOCABULARY_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_source_to_concept_map_target_vocabulary_id' as checkid
 ,0 as is_error
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
 'SOURCE_TO_CONCEPT_MAP.TARGET_VOCABULARY_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SOURCE_TO_CONCEPT_MAP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VOCABULARY fkTable
 ON cdmTable.TARGET_VOCABULARY_ID = fkTable.VOCABULARY_ID
 WHERE fkTable.VOCABULARY_ID IS NULL
 AND cdmTable.TARGET_VOCABULARY_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the ANATOMIC_SITE_CONCEPT_ID field in the SPECIMEN table that does not exist in the CONCEPT table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'ANATOMIC_SITE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_specimen_anatomic_site_concept_id' as checkid
 ,0 as is_error
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
 'SPECIMEN.ANATOMIC_SITE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.ANATOMIC_SITE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.ANATOMIC_SITE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the DISEASE_STATUS_CONCEPT_ID field in the SPECIMEN table that does not exist in the CONCEPT table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'DISEASE_STATUS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_specimen_disease_status_concept_id' as checkid
 ,0 as is_error
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
 'SPECIMEN.DISEASE_STATUS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.DISEASE_STATUS_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.DISEASE_STATUS_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the SPECIMEN table that does not exist in the PERSON table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_specimen_person_id' as checkid
 ,0 as is_error
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
 'SPECIMEN.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SPECIMEN_CONCEPT_ID field in the SPECIMEN table that does not exist in the CONCEPT table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_specimen_specimen_concept_id' as checkid
 ,0 as is_error
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
 'SPECIMEN.SPECIMEN_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SPECIMEN_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SPECIMEN_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SPECIMEN_TYPE_CONCEPT_ID field in the SPECIMEN table that does not exist in the CONCEPT table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_specimen_specimen_type_concept_id' as checkid
 ,0 as is_error
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
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SPECIMEN_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SPECIMEN_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the UNIT_CONCEPT_ID field in the SPECIMEN table that does not exist in the CONCEPT table.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_specimen_unit_concept_id' as checkid
 ,0 as is_error
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
 'SPECIMEN.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.SPECIMEN cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.UNIT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the ADMITTED_FROM_CONCEPT_ID field in the VISIT_DETAIL table that does not exist in the CONCEPT table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'ADMITTED_FROM_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_admitted_from_concept_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.ADMITTED_FROM_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.ADMITTED_FROM_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.ADMITTED_FROM_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the CARE_SITE_ID field in the VISIT_DETAIL table that does not exist in the CARE_SITE table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_care_site_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CARE_SITE fkTable
 ON cdmTable.CARE_SITE_ID = fkTable.CARE_SITE_ID
 WHERE fkTable.CARE_SITE_ID IS NULL
 AND cdmTable.CARE_SITE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the DISCHARGED_TO_CONCEPT_ID field in the VISIT_DETAIL table that does not exist in the CONCEPT table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_discharged_to_concept_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.DISCHARGED_TO_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.DISCHARGED_TO_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.DISCHARGED_TO_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PARENT_VISIT_DETAIL_ID field in the VISIT_DETAIL table that does not exist in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PARENT_VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_parent_visit_detail_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.PARENT_VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_DETAIL fkTable
 ON cdmTable.PARENT_VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL
 AND cdmTable.PARENT_VISIT_DETAIL_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the VISIT_DETAIL table that does not exist in the PERSON table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_person_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PRECEDING_VISIT_DETAIL_ID field in the VISIT_DETAIL table that does not exist in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PRECEDING_VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_preceding_visit_detail_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.PRECEDING_VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_DETAIL fkTable
 ON cdmTable.PRECEDING_VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL
 AND cdmTable.PRECEDING_VISIT_DETAIL_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the VISIT_DETAIL table that does not exist in the PROVIDER table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_visit_detail_provider_id' as checkid
 ,0 as is_error
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
 'VISIT_DETAIL.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PROVIDER fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.VISIT_DETAIL cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
