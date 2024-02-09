
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'isStandardValidConcept' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records that do not have a standard, valid concept in the VISIT_DETAIL_CONCEPT_ID field in the VISIT_DETAIL table.' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_is_standard_valid_concept.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_isstandardvalidconcept_visit_detail_visit_detail_concept_id' as checkid
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
 'VISIT_DETAIL.VISIT_DETAIL_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'visit_detail' ) }} cdmTable
 JOIN {{ source('omop',  'concept' ) }} co
 ON cdmTable.VISIT_DETAIL_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'visit_detail' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
