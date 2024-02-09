WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if PERSON table is present as expected based on the specification.' as check_description
 ,'PERSON' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_person' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.PERSON cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if OBSERVATION_PERIOD table is present as expected based on the specification.' as check_description
 ,'OBSERVATION_PERIOD' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_observation_period' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.OBSERVATION_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if VISIT_OCCURRENCE table is present as expected based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_visit_occurrence' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.VISIT_OCCURRENCE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if VISIT_DETAIL table is present as expected based on the specification.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_visit_detail' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.VISIT_DETAIL cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if CONDITION_OCCURRENCE table is present as expected based on the specification.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_condition_occurrence' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.CONDITION_OCCURRENCE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if DRUG_EXPOSURE table is present as expected based on the specification.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_drug_exposure' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.DRUG_EXPOSURE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if PROCEDURE_OCCURRENCE table is present as expected based on the specification.' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_procedure_occurrence' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.PROCEDURE_OCCURRENCE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if DEVICE_EXPOSURE table is present as expected based on the specification.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_device_exposure' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.DEVICE_EXPOSURE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if MEASUREMENT table is present as expected based on the specification.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_measurement' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.MEASUREMENT cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if OBSERVATION table is present as expected based on the specification.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_observation' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.OBSERVATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if DEATH table is present as expected based on the specification.' as check_description
 ,'DEATH' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_death' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.DEATH cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if NOTE table is present as expected based on the specification.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_note' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.NOTE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if NOTE_NLP table is present as expected based on the specification.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_note_nlp' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.NOTE_NLP cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if SPECIMEN table is present as expected based on the specification.' as check_description
 ,'SPECIMEN' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_specimen' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.SPECIMEN cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if FACT_RELATIONSHIP table is present as expected based on the specification.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_fact_relationship' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.FACT_RELATIONSHIP cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if LOCATION table is present as expected based on the specification.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_location' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.LOCATION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if CARE_SITE table is present as expected based on the specification.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_care_site' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.CARE_SITE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if PROVIDER table is present as expected based on the specification.' as check_description
 ,'PROVIDER' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_provider' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.PROVIDER cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if PAYER_PLAN_PERIOD table is present as expected based on the specification.' as check_description
 ,'PAYER_PLAN_PERIOD' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_payer_plan_period' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.PAYER_PLAN_PERIOD cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if COST table is present as expected based on the specification.' as check_description
 ,'COST' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_cost' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.COST cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if DRUG_ERA table is present as expected based on the specification.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_drug_era' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.DRUG_ERA cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if DOSE_ERA table is present as expected based on the specification.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_dose_era' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.DOSE_ERA cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if CONDITION_ERA table is present as expected based on the specification.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_condition_era' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.CONDITION_ERA cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if EPISODE table is present as expected based on the specification.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_episode' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.EPISODE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if EPISODE_EVENT table is present as expected based on the specification.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_episode_event' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.EPISODE_EVENT cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if METADATA table is present as expected based on the specification.' as check_description
 ,'METADATA' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_metadata' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.METADATA cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if CDM_SOURCE table is present as expected based on the specification.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_cdm_source' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.CDM_SOURCE cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if SOURCE_TO_CONCEPT_MAP table is present as expected based on the specification.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_source_to_concept_map' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.SOURCE_TO_CONCEPT_MAP cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if DRUG_STRENGTH table is present as expected based on the specification.' as check_description
 ,'DRUG_STRENGTH' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_drug_strength' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.omop_source.DRUG_STRENGTH cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if COHORT table is present as expected based on the specification.' as check_description
 ,'COHORT' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_cohort' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
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
 ,'cdmTable' as check_name
 ,'TABLE' as check_level
 ,'A yes or no value indicating if COHORT_DEFINITION table is present as expected based on the specification.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_cdm_table.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'table_cdmtable_cohort_definition' as checkid
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
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte
)
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
