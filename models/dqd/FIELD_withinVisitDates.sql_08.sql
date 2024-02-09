
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'withinVisitDates' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
 ,'VISIT_DETAIL' as cdm_table_name
 ,'VISIT_DETAIL_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_within_visit_dates.sql' as sql_file
 ,'Conformance' as category
 ,'NA' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_withinvisitdates_visit_detail_visit_detail_start_date' as checkid
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
 FROM (
 /*violatedRowsBegin*/
 SELECT
 'VISIT_DETAIL.VISIT_DETAIL_START_DATE' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'visit_detail' ) }} cdmTable
 JOIN {{ source('omop',  'visit_occurrence' ) }} vo
 ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
 WHERE cdmTable.VISIT_DETAIL_START_DATE < date_add(vo.visit_start_date, -7)
 OR cdmTable.VISIT_DETAIL_START_DATE > date_add(vo.visit_end_date, 7)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'visit_detail' ) }} cdmTable
 JOIN {{ source('omop',  'visit_occurrence' ) }} vo
 ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
) denominator
) cte
)

SELECT *
FROM cte_all
