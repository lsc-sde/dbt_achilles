
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
 FROM {{ source('omop',  'visit_detail' ) }} cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte
)

SELECT *
FROM cte_all
