
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleTemporalAfter' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value in the VISIT_END_DATETIME field of the VISIT_OCCURRENCE that occurs prior to the date in the VISIT_START_DATETIME field of the VISIT_OCCURRENCE table.' as check_description
 ,'VISIT_OCCURRENCE' as cdm_table_name
 ,'VISIT_END_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_plausible_temporal_after.sql' as sql_file
 ,'Plausibility' as category
 ,'Temporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_plausibletemporalafter_visit_occurrence_visit_end_datetime' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,1 as threshold_value
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
 'VISIT_OCCURRENCE.VISIT_END_DATETIME' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'visit_occurrence' ) }} cdmTable
 WHERE
 CAST(cdmTable.VISIT_START_DATETIME AS DATE)
 > CAST(cdmTable.VISIT_END_DATETIME AS DATE)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'visit_occurrence' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
