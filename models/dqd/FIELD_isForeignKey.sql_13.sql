
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isForeignKey' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that have a value in the PROVIDER_ID field in the CONDITION_OCCURRENCE table that does not exist in the PROVIDER table.' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'PROVIDER_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'is_foreign_key.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isforeignkey_condition_occurrence_provider_id' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'CONDITION_OCCURRENCE.PROVIDER_ID' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'condition_occurrence' ) }} cdmTable
 LEFT JOIN
 {{ source('omop',  'provider' ) }} fkTable
 ON cdmTable.PROVIDER_ID = fkTable.PROVIDER_ID
 WHERE fkTable.PROVIDER_ID IS NULL
 AND cdmTable.PROVIDER_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'condition_occurrence' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
