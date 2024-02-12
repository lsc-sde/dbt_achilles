WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CARE_SITE_ID of the CARE_SITE that is considered not nullable.' as check_description
 ,'CARE_SITE' as cdm_table_name
 ,'CARE_SITE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_care_site_care_site_id' as checkid
 ,0 as is_error
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
 'CARE_SITE.CARE_SITE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CARE_SITE cdmTable
 WHERE cdmTable.CARE_SITE_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CDM_HOLDER of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_HOLDER' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_cdm_holder' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.CDM_HOLDER' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.CDM_HOLDER IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CDM_RELEASE_DATE of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_RELEASE_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_cdm_release_date' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.CDM_RELEASE_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.CDM_RELEASE_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CDM_SOURCE_ABBREVIATION of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_SOURCE_ABBREVIATION' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_cdm_source_abbreviation' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.CDM_SOURCE_ABBREVIATION' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.CDM_SOURCE_ABBREVIATION IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CDM_SOURCE_NAME of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_SOURCE_NAME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_cdm_source_name' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.CDM_SOURCE_NAME' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.CDM_SOURCE_NAME IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CDM_VERSION_CONCEPT_ID of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'CDM_VERSION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_cdm_version_concept_id' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.CDM_VERSION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.CDM_VERSION_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SOURCE_RELEASE_DATE of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'SOURCE_RELEASE_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_source_release_date' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.SOURCE_RELEASE_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.SOURCE_RELEASE_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the VOCABULARY_VERSION of the CDM_SOURCE that is considered not nullable.' as check_description
 ,'CDM_SOURCE' as cdm_table_name
 ,'VOCABULARY_VERSION' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cdm_source_vocabulary_version' as checkid
 ,0 as is_error
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
 'CDM_SOURCE.VOCABULARY_VERSION' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CDM_SOURCE cdmTable
 WHERE cdmTable.VOCABULARY_VERSION IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COHORT_DEFINITION_ID of the COHORT that is considered not nullable.' as check_description
 ,'COHORT' as cdm_table_name
 ,'COHORT_DEFINITION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_cohort_definition_id' as checkid
 ,0 as is_error
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
 'COHORT.COHORT_DEFINITION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 WHERE cdmTable.COHORT_DEFINITION_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COHORT_END_DATE of the COHORT that is considered not nullable.' as check_description
 ,'COHORT' as cdm_table_name
 ,'COHORT_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_cohort_end_date' as checkid
 ,0 as is_error
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
 'COHORT.COHORT_END_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 WHERE cdmTable.COHORT_END_DATE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COHORT_START_DATE of the COHORT that is considered not nullable.' as check_description
 ,'COHORT' as cdm_table_name
 ,'COHORT_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_cohort_start_date' as checkid
 ,0 as is_error
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
 'COHORT.COHORT_START_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 WHERE cdmTable.COHORT_START_DATE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SUBJECT_ID of the COHORT that is considered not nullable.' as check_description
 ,'COHORT' as cdm_table_name
 ,'SUBJECT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_subject_id' as checkid
 ,0 as is_error
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
 'COHORT.SUBJECT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
 WHERE cdmTable.SUBJECT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc_achilles.COHORT cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COHORT_DEFINITION_ID of the COHORT_DEFINITION that is considered not nullable.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'COHORT_DEFINITION_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_definition_cohort_definition_id' as checkid
 ,0 as is_error
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
 'COHORT_DEFINITION.COHORT_DEFINITION_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE cdmTable.COHORT_DEFINITION_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COHORT_DEFINITION_NAME of the COHORT_DEFINITION that is considered not nullable.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'COHORT_DEFINITION_NAME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_definition_cohort_definition_name' as checkid
 ,0 as is_error
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
 'COHORT_DEFINITION.COHORT_DEFINITION_NAME' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE cdmTable.COHORT_DEFINITION_NAME IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEFINITION_TYPE_CONCEPT_ID of the COHORT_DEFINITION that is considered not nullable.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'DEFINITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_definition_definition_type_concept_id' as checkid
 ,0 as is_error
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
 'COHORT_DEFINITION.DEFINITION_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE cdmTable.DEFINITION_TYPE_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SUBJECT_CONCEPT_ID of the COHORT_DEFINITION that is considered not nullable.' as check_description
 ,'COHORT_DEFINITION' as cdm_table_name
 ,'SUBJECT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cohort_definition_subject_concept_id' as checkid
 ,0 as is_error
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
 'COHORT_DEFINITION.SUBJECT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc_achilles.COHORT_DEFINITION cdmTable
 WHERE cdmTable.SUBJECT_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_CONCEPT_ID of the CONDITION_ERA that is considered not nullable.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_era_condition_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.CONDITION_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_ERA_END_DATE of the CONDITION_ERA that is considered not nullable.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_ERA_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_era_condition_era_end_date' as checkid
 ,0 as is_error
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
 'CONDITION_ERA.CONDITION_ERA_END_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE cdmTable.CONDITION_ERA_END_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_ERA_ID of the CONDITION_ERA that is considered not nullable.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_era_condition_era_id' as checkid
 ,0 as is_error
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
 'CONDITION_ERA.CONDITION_ERA_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE cdmTable.CONDITION_ERA_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_ERA_START_DATE of the CONDITION_ERA that is considered not nullable.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_ERA_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_era_condition_era_start_date' as checkid
 ,0 as is_error
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
 'CONDITION_ERA.CONDITION_ERA_START_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE cdmTable.CONDITION_ERA_START_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the CONDITION_ERA that is considered not nullable.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_era_person_id' as checkid
 ,0 as is_error
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
 'CONDITION_ERA.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_ERA cdmTable
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_CONCEPT_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_occurrence_condition_concept_id' as checkid
 ,0 as is_error
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
 'CONDITION_OCCURRENCE.CONDITION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_OCCURRENCE_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_occurrence_condition_occurrence_id' as checkid
 ,0 as is_error
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
 'CONDITION_OCCURRENCE.CONDITION_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_OCCURRENCE_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_START_DATE of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_occurrence_condition_start_date' as checkid
 ,0 as is_error
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
 'CONDITION_OCCURRENCE.CONDITION_START_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.CONDITION_START_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the CONDITION_TYPE_CONCEPT_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_occurrence_condition_type_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.CONDITION_TYPE_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_condition_occurrence_person_id' as checkid
 ,0 as is_error
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
 'CONDITION_OCCURRENCE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COST_DOMAIN_ID of the COST that is considered not nullable.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_DOMAIN_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cost_cost_domain_id' as checkid
 ,0 as is_error
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
 'COST.COST_DOMAIN_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE cdmTable.COST_DOMAIN_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COST_EVENT_ID of the COST that is considered not nullable.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_EVENT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cost_cost_event_id' as checkid
 ,0 as is_error
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
 'COST.COST_EVENT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE cdmTable.COST_EVENT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COST_ID of the COST that is considered not nullable.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cost_cost_id' as checkid
 ,0 as is_error
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
 'COST.COST_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.COST cdmTable
 WHERE cdmTable.COST_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the COST_TYPE_CONCEPT_ID of the COST that is considered not nullable.' as check_description
 ,'COST' as cdm_table_name
 ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_cost_cost_type_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.COST_TYPE_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEATH_DATE of the DEATH that is considered not nullable.' as check_description
 ,'DEATH' as cdm_table_name
 ,'DEATH_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_death_death_date' as checkid
 ,0 as is_error
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
 'DEATH.DEATH_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE cdmTable.DEATH_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the DEATH that is considered not nullable.' as check_description
 ,'DEATH' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_death_person_id' as checkid
 ,0 as is_error
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
 'DEATH.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEATH cdmTable
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEVICE_CONCEPT_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_device_exposure_device_concept_id' as checkid
 ,0 as is_error
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
 'DEVICE_EXPOSURE.DEVICE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.DEVICE_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEVICE_EXPOSURE_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_device_exposure_device_exposure_id' as checkid
 ,0 as is_error
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
 'DEVICE_EXPOSURE.DEVICE_EXPOSURE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.DEVICE_EXPOSURE_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEVICE_EXPOSURE_START_DATE of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_device_exposure_device_exposure_start_date' as checkid
 ,0 as is_error
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
 'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.DEVICE_EXPOSURE_START_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DEVICE_TYPE_CONCEPT_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_device_exposure_device_type_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.DEVICE_TYPE_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
 ,'DEVICE_EXPOSURE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_device_exposure_person_id' as checkid
 ,0 as is_error
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
 'DEVICE_EXPOSURE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DEVICE_EXPOSURE cdmTable
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DOSE_ERA_END_DATE of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_dose_era_end_date' as checkid
 ,0 as is_error
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
 'DOSE_ERA.DOSE_ERA_END_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.DOSE_ERA_END_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DOSE_ERA_ID of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_dose_era_id' as checkid
 ,0 as is_error
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
 'DOSE_ERA.DOSE_ERA_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.DOSE_ERA_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DOSE_ERA_START_DATE of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_dose_era_start_date' as checkid
 ,0 as is_error
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
 'DOSE_ERA.DOSE_ERA_START_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.DOSE_ERA_START_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DOSE_VALUE of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_dose_value' as checkid
 ,0 as is_error
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
 'DOSE_ERA.DOSE_VALUE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.DOSE_VALUE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DRUG_CONCEPT_ID of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_drug_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.DRUG_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_person_id' as checkid
 ,0 as is_error
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
 'DOSE_ERA.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DOSE_ERA cdmTable
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the UNIT_CONCEPT_ID of the DOSE_ERA that is considered not nullable.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_dose_era_unit_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.UNIT_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DRUG_CONCEPT_ID of the DRUG_ERA that is considered not nullable.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_drug_era_drug_concept_id' as checkid
 ,0 as is_error
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
 WHERE cdmTable.DRUG_CONCEPT_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DRUG_ERA_END_DATE of the DRUG_ERA that is considered not nullable.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_ERA_END_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_drug_era_drug_era_end_date' as checkid
 ,0 as is_error
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
 'DRUG_ERA.DRUG_ERA_END_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE cdmTable.DRUG_ERA_END_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DRUG_ERA_ID of the DRUG_ERA that is considered not nullable.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_drug_era_drug_era_id' as checkid
 ,0 as is_error
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
 'DRUG_ERA.DRUG_ERA_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE cdmTable.DRUG_ERA_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DRUG_ERA_START_DATE of the DRUG_ERA that is considered not nullable.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'DRUG_ERA_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_drug_era_drug_era_start_date' as checkid
 ,0 as is_error
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
 'DRUG_ERA.DRUG_ERA_START_DATE' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE cdmTable.DRUG_ERA_START_DATE IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the PERSON_ID of the DRUG_ERA that is considered not nullable.' as check_description
 ,'DRUG_ERA' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_drug_era_person_id' as checkid
 ,0 as is_error
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
 'DRUG_ERA.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_ERA cdmTable
 WHERE cdmTable.PERSON_ID IS NULL
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
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the DRUG_CONCEPT_ID of the DRUG_EXPOSURE that is considered not nullable.' as check_description
 ,'DRUG_EXPOSURE' as cdm_table_name
 ,'DRUG_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_drug_exposure_drug_concept_id' as checkid
 ,0 as is_error
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
 'DRUG_EXPOSURE.DRUG_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
 WHERE cdmTable.DRUG_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_EXPOSURE cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
