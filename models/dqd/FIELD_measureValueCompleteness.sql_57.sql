
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'measureValueCompleteness' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the REVENUE_CODE_SOURCE_VALUE of the COST.' as check_description
 ,'COST' as cdm_table_name
 ,'REVENUE_CODE_SOURCE_VALUE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_measure_value_completeness.sql' as sql_file
 ,'Completeness' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_measurevaluecompleteness_cost_revenue_code_source_value' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,100 as threshold_value
 ,NULL as notes_value
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
 'COST.REVENUE_CODE_SOURCE_VALUE' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'cost' ) }} cdmTable
 WHERE cdmTable.REVENUE_CODE_SOURCE_VALUE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'cost' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
