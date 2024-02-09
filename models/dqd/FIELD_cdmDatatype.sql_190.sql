
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'cdmDatatype' as check_name
 ,'FIELD' as check_level
 ,'A yes or no value indicating if the DISCHARGED_TO_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_cdm_datatype.sql' as sql_file
 ,'Conformance' as category
 ,'Value' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_cdmdatatype_visit_occurrence_discharged_to_concept_id' as checkid
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
 'VISIT_OCCURRENCE.DISCHARGED_TO_CONCEPT_ID' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'visit_occurrence' ) }} cdmTable
 WHERE
 (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0
 OR (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1
 AND INSTR(CAST(ABS(cdmTable.DISCHARGED_TO_CONCEPT_ID) AS STRING),'.') != 0))
 AND cdmTable.DISCHARGED_TO_CONCEPT_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'visit_occurrence' ) }}
) denominator
) cte
)

SELECT *
FROM cte_all
