
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isPrimaryKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a duplicate value in the CONDITION_ERA_ID field of the CONDITION_ERA.' as check_description
 ,'CONDITION_ERA' as cdm_table_name
 ,'CONDITION_ERA_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_primary_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isprimarykey_condition_era_condition_era_id' as checkid
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
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 'CONDITION_ERA.CONDITION_ERA_ID' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'condition_era' ) }} cdmTable
 WHERE cdmTable.CONDITION_ERA_ID IN (
 SELECT
 CONDITION_ERA_ID
 FROM {{ source('omop',  'condition_era' ) }}
 GROUP BY CONDITION_ERA_ID
 HAVING COUNT(*) > 1
 )
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'condition_era' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
