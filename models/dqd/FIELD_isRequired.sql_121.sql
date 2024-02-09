
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isRequired' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a NULL value in the SOURCE_CONCEPT_ID of the SOURCE_TO_CONCEPT_MAP that is considered not nullable.' as check_description
 ,'SOURCE_TO_CONCEPT_MAP' as cdm_table_name
 ,'SOURCE_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_not_nullable.sql' as sql_file
 ,'Conformance' as category
 ,'Relational' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'field_isrequired_source_to_concept_map_source_concept_id' as checkid
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
 'SOURCE_TO_CONCEPT_MAP.SOURCE_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'source_to_concept_map' ) }} cdmTable
 WHERE cdmTable.SOURCE_CONCEPT_ID IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'source_to_concept_map' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
