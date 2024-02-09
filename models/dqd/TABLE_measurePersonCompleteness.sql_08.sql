
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'measurePersonCompleteness' as check_name
 ,'TABLE' as check_level
 ,'The number and percent of persons in the CDM that do not have at least one record in the OBSERVATION table' as check_description
 ,'OBSERVATION' as cdm_table_name
 ,'NA' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'table_person_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'table_measurepersoncompleteness_observation' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,95 as threshold_value
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
 /*violatedRowsBegin*/
 SELECT
 cdmTable.*
 FROM {{ source('omop',  'person' ) }} cdmTable
 LEFT JOIN {{ source('omop',  'observation' ) }} cdmTable2
 ON cdmTable.person_id = cdmTable2.person_id
 WHERE cdmTable2.person_id IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'person' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
