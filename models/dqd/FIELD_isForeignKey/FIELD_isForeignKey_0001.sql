WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the INGREDIENT_CONCEPT_ID field in the DRUG_STRENGTH table that does not exist in the CONCEPT table.' as check_description
 ,'DRUG_STRENGTH' as cdm_table_name
 ,'INGREDIENT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_drug_strength_ingredient_concept_id' as checkid
 ,0 as is_error
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
 'DRUG_STRENGTH.INGREDIENT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_STRENGTH cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.INGREDIENT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.INGREDIENT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_STRENGTH cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the NUMERATOR_UNIT_CONCEPT_ID field in the DRUG_STRENGTH table that does not exist in the CONCEPT table.' as check_description
 ,'DRUG_STRENGTH' as cdm_table_name
 ,'NUMERATOR_UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_drug_strength_numerator_unit_concept_id' as checkid
 ,0 as is_error
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
 'DRUG_STRENGTH.NUMERATOR_UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.DRUG_STRENGTH cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.NUMERATOR_UNIT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.NUMERATOR_UNIT_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.DRUG_STRENGTH cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the EPISODE_CONCEPT_ID field in the EPISODE table that does not exist in the CONCEPT table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_episode_concept_id' as checkid
 ,0 as is_error
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
 'EPISODE.EPISODE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.EPISODE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.EPISODE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the EPISODE_OBJECT_CONCEPT_ID field in the EPISODE table that does not exist in the CONCEPT table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_OBJECT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_episode_object_concept_id' as checkid
 ,0 as is_error
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
 'EPISODE.EPISODE_OBJECT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.EPISODE_OBJECT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.EPISODE_OBJECT_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the EPISODE_SOURCE_CONCEPT_ID field in the EPISODE table that does not exist in the CONCEPT table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_episode_source_concept_id' as checkid
 ,0 as is_error
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
 'EPISODE.EPISODE_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.EPISODE_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.EPISODE_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the EPISODE_TYPE_CONCEPT_ID field in the EPISODE table that does not exist in the CONCEPT table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'EPISODE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_episode_type_concept_id' as checkid
 ,0 as is_error
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
 'EPISODE.EPISODE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.EPISODE_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.EPISODE_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the EPISODE table that does not exist in the PERSON table.' as check_description
 ,'EPISODE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_person_id' as checkid
 ,0 as is_error
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
 'EPISODE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the EPISODE_EVENT_FIELD_CONCEPT_ID field in the EPISODE_EVENT table that does not exist in the CONCEPT table.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'EPISODE_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_event_episode_event_field_concept_id' as checkid
 ,0 as is_error
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
 'EPISODE_EVENT.EPISODE_EVENT_FIELD_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.EPISODE_EVENT_FIELD_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.EPISODE_EVENT_FIELD_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the EPISODE_ID field in the EPISODE_EVENT table that does not exist in the EPISODE table.' as check_description
 ,'EPISODE_EVENT' as cdm_table_name
 ,'EPISODE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_episode_event_episode_id' as checkid
 ,0 as is_error
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
 'EPISODE_EVENT.EPISODE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.EPISODE_EVENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.EPISODE fkTable
 ON cdmTable.EPISODE_ID = fkTable.EPISODE_ID
 WHERE fkTable.EPISODE_ID IS NULL
 AND cdmTable.EPISODE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the DOMAIN_CONCEPT_ID_1 field in the FACT_RELATIONSHIP table that does not exist in the CONCEPT table.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'DOMAIN_CONCEPT_ID_1' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_fact_relationship_domain_concept_id_1' as checkid
 ,0 as is_error
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
 'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_1' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.DOMAIN_CONCEPT_ID_1 = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.DOMAIN_CONCEPT_ID_1 IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the DOMAIN_CONCEPT_ID_2 field in the FACT_RELATIONSHIP table that does not exist in the CONCEPT table.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'DOMAIN_CONCEPT_ID_2' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_fact_relationship_domain_concept_id_2' as checkid
 ,0 as is_error
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
 'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_2' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.DOMAIN_CONCEPT_ID_2 = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.DOMAIN_CONCEPT_ID_2 IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the RELATIONSHIP_CONCEPT_ID field in the FACT_RELATIONSHIP table that does not exist in the CONCEPT table.' as check_description
 ,'FACT_RELATIONSHIP' as cdm_table_name
 ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_fact_relationship_relationship_concept_id' as checkid
 ,0 as is_error
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
 'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.RELATIONSHIP_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.RELATIONSHIP_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.FACT_RELATIONSHIP cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the COUNTRY_CONCEPT_ID field in the LOCATION table that does not exist in the CONCEPT table.' as check_description
 ,'LOCATION' as cdm_table_name
 ,'COUNTRY_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_location_country_concept_id' as checkid
 ,0 as is_error
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
 'LOCATION.COUNTRY_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.LOCATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.COUNTRY_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.COUNTRY_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the MEAS_EVENT_FIELD_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEAS_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_meas_event_field_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.MEAS_EVENT_FIELD_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.MEAS_EVENT_FIELD_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.MEAS_EVENT_FIELD_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the MEASUREMENT_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_measurement_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.MEASUREMENT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.MEASUREMENT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.MEASUREMENT_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the MEASUREMENT_SOURCE_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_measurement_source_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.MEASUREMENT_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the MEASUREMENT_TYPE_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_measurement_type_concept_id' as checkid
 ,0 as is_error
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
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.MEASUREMENT_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.MEASUREMENT_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the OPERATOR_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'OPERATOR_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_operator_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.OPERATOR_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.OPERATOR_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.OPERATOR_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the MEASUREMENT table that does not exist in the PERSON table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_person_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the MEASUREMENT table that does not exist in the PROVIDER table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_provider_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PROVIDER fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the UNIT_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_unit_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.UNIT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the UNIT_SOURCE_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'UNIT_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_unit_source_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.UNIT_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.UNIT_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.UNIT_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VALUE_AS_CONCEPT_ID field in the MEASUREMENT table that does not exist in the CONCEPT table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'VALUE_AS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_value_as_concept_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.VALUE_AS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.VALUE_AS_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.VALUE_AS_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_DETAIL_ID field in the MEASUREMENT table that does not exist in the VISIT_DETAIL table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_visit_detail_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_DETAIL fkTable
 ON cdmTable.VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_OCCURRENCE_ID field in the MEASUREMENT table that does not exist in the VISIT_OCCURRENCE table.' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_measurement_visit_occurrence_id' as checkid
 ,0 as is_error
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
 'MEASUREMENT.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.MEASUREMENT cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_OCCURRENCE fkTable
 ON cdmTable.VISIT_OCCURRENCE_ID = fkTable.VISIT_OCCURRENCE_ID
 WHERE fkTable.VISIT_OCCURRENCE_ID IS NULL
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the METADATA_CONCEPT_ID field in the METADATA table that does not exist in the CONCEPT table.' as check_description
 ,'METADATA' as cdm_table_name
 ,'METADATA_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_metadata_metadata_concept_id' as checkid
 ,0 as is_error
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
 'METADATA.METADATA_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.METADATA cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.METADATA_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.METADATA_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.METADATA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the METADATA_TYPE_CONCEPT_ID field in the METADATA table that does not exist in the CONCEPT table.' as check_description
 ,'METADATA' as cdm_table_name
 ,'METADATA_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_metadata_metadata_type_concept_id' as checkid
 ,0 as is_error
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
 'METADATA.METADATA_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.METADATA cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.METADATA_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.METADATA_TYPE_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.METADATA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VALUE_AS_CONCEPT_ID field in the METADATA table that does not exist in the CONCEPT table.' as check_description
 ,'METADATA' as cdm_table_name
 ,'VALUE_AS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_metadata_value_as_concept_id' as checkid
 ,0 as is_error
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
 'METADATA.VALUE_AS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.METADATA cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.VALUE_AS_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.VALUE_AS_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.METADATA cdmTable
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the ENCODING_CONCEPT_ID field in the NOTE table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'ENCODING_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_encoding_concept_id' as checkid
 ,0 as is_error
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
 'NOTE.ENCODING_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.ENCODING_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.ENCODING_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the LANGUAGE_CONCEPT_ID field in the NOTE table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'LANGUAGE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_language_concept_id' as checkid
 ,0 as is_error
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
 'NOTE.LANGUAGE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.LANGUAGE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.LANGUAGE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the NOTE_CLASS_CONCEPT_ID field in the NOTE table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_CLASS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_note_class_concept_id' as checkid
 ,0 as is_error
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
 'NOTE.NOTE_CLASS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.NOTE_CLASS_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.NOTE_CLASS_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the NOTE_EVENT_FIELD_CONCEPT_ID field in the NOTE table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_note_event_field_concept_id' as checkid
 ,0 as is_error
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
 'NOTE.NOTE_EVENT_FIELD_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.NOTE_EVENT_FIELD_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the NOTE_TYPE_CONCEPT_ID field in the NOTE table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'NOTE_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_note_type_concept_id' as checkid
 ,0 as is_error
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
 'NOTE.NOTE_TYPE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.NOTE_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.NOTE_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PERSON_ID field in the NOTE table that does not exist in the PERSON table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_person_id' as checkid
 ,0 as is_error
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
 'NOTE.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the NOTE table that does not exist in the PROVIDER table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_provider_id' as checkid
 ,0 as is_error
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
 'NOTE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PROVIDER fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_DETAIL_ID field in the NOTE table that does not exist in the VISIT_DETAIL table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_visit_detail_id' as checkid
 ,0 as is_error
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
 'NOTE.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_DETAIL fkTable
 ON cdmTable.VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the VISIT_OCCURRENCE_ID field in the NOTE table that does not exist in the VISIT_OCCURRENCE table.' as check_description
 ,'NOTE' as cdm_table_name
 ,'VISIT_OCCURRENCE_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_visit_occurrence_id' as checkid
 ,0 as is_error
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
 'NOTE.VISIT_OCCURRENCE_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_OCCURRENCE fkTable
 ON cdmTable.VISIT_OCCURRENCE_ID = fkTable.VISIT_OCCURRENCE_ID
 WHERE fkTable.VISIT_OCCURRENCE_ID IS NULL
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the NOTE_NLP_CONCEPT_ID field in the NOTE_NLP table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_NLP_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_nlp_note_nlp_concept_id' as checkid
 ,0 as is_error
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
 'NOTE_NLP.NOTE_NLP_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.NOTE_NLP_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.NOTE_NLP_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the NOTE_NLP_SOURCE_CONCEPT_ID field in the NOTE_NLP table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NOTE_NLP_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_nlp_note_nlp_source_concept_id' as checkid
 ,0 as is_error
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
 'NOTE_NLP.NOTE_NLP_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the SECTION_CONCEPT_ID field in the NOTE_NLP table that does not exist in the CONCEPT table.' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'SECTION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_note_nlp_section_concept_id' as checkid
 ,0 as is_error
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
 'NOTE_NLP.SECTION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.NOTE_NLP cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.SECTION_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.SECTION_CONCEPT_ID IS NOT NULL
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
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the OBS_EVENT_FIELD_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBS_EVENT_FIELD_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_obs_event_field_concept_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.OBS_EVENT_FIELD_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.OBS_EVENT_FIELD_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.OBS_EVENT_FIELD_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the OBSERVATION_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_observation_concept_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.OBSERVATION_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.OBSERVATION_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.OBSERVATION_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the OBSERVATION_SOURCE_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_observation_source_concept_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.OBSERVATION_SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.OBSERVATION_SOURCE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.OBSERVATION_SOURCE_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the OBSERVATION_TYPE_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_observation_type_concept_id' as checkid
 ,0 as is_error
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
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.OBSERVATION_TYPE_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.OBSERVATION_TYPE_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the PERSON_ID field in the OBSERVATION table that does not exist in the PERSON table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'PERSON_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_person_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.PERSON_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PERSON fkTable
 ON cdmTable.PERSON_ID = fkTable.PERSON_ID
 WHERE fkTable.PERSON_ID IS NULL
 AND cdmTable.PERSON_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the OBSERVATION table that does not exist in the PROVIDER table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_provider_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.PROVIDER fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the QUALIFIER_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'QUALIFIER_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_qualifier_concept_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.QUALIFIER_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.QUALIFIER_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.QUALIFIER_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the UNIT_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'UNIT_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_unit_concept_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.UNIT_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.UNIT_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the VALUE_AS_CONCEPT_ID field in the OBSERVATION table that does not exist in the CONCEPT table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VALUE_AS_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_value_as_concept_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.VALUE_AS_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.CONCEPT fkTable
 ON cdmTable.VALUE_AS_CONCEPT_ID = fkTable.CONCEPT_ID
 WHERE fkTable.CONCEPT_ID IS NULL
 AND cdmTable.VALUE_AS_CONCEPT_ID IS NOT NULL
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
 ,'The number and percent of records that have a value in the VISIT_DETAIL_ID field in the OBSERVATION table that does not exist in the VISIT_DETAIL table.' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'VISIT_DETAIL_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_observation_visit_detail_id' as checkid
 ,0 as is_error
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
 'OBSERVATION.VISIT_DETAIL_ID' AS violating_field,
 cdmTable.*
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
 LEFT JOIN
 hive_metastore.dev_vc.VISIT_DETAIL fkTable
 ON cdmTable.VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.OBSERVATION cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
