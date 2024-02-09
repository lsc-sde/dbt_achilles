WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'measureConditionEraCompleteness' as check_name
 ,'TABLE' as check_level
 ,'The number and Percent of persons that does not have condition_era built successfully for all persons in condition_occurrence' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_condition_era_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'table_measureconditioneracompleteness_condition_era' as checkid
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
 COUNT(violated_rows.person_id) AS num_violated_rows
 FROM
 (
 SELECT DISTINCT
 co.person_id
 FROM {{ source('omop',  'condition_occurrence' ) }} co
 LEFT JOIN {{ source('omop',  'condition_era' ) }} cdmTable
 ON co.person_id = cdmTable.person_id
 WHERE cdmTable.person_id IS NULL
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(DISTINCT person_id) AS num_rows
 FROM {{ source('omop',  'condition_occurrence' ) }} co
) denominator
) cte
)

SELECT *
FROM cte_all
