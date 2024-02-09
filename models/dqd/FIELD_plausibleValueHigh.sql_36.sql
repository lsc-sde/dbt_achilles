
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleValueHigh' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value in the NLP_DATETIME field of the NOTE_NLP table greater than DATEADD(DD,1,GETDATE()).' as check_description
 ,'NOTE_NLP' as cdm_table_name
 ,'NLP_DATETIME' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_plausible_value_high.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_plausiblevaluehigh_note_nlp_nlp_datetime' as checkid
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
 SELECT 'NOTE_NLP.NLP_DATETIME' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'note_nlp' ) }} cdmTable
 WHERE cast(cdmTable.NLP_DATETIME as date) > cast(date_add(CURRENT_DATE, 1) as date)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'note_nlp' ) }} cdmTable
 WHERE NLP_DATETIME IS NOT NULL
) denominator
) cte
)

SELECT *
FROM cte_all
