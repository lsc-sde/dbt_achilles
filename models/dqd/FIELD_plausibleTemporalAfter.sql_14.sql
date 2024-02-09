
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleTemporalAfter' as check_name
 ,'FIELD' as check_level
 ,'The number and percent of records with a value in the DOSE_ERA_START_DATE field of the DOSE_ERA that occurs prior to the date in the BIRTH_DATETIME field of the PERSON table.' as check_description
 ,'DOSE_ERA' as cdm_table_name
 ,'DOSE_ERA_START_DATE' as cdm_field_name
 ,'NA' as concept_id
 ,'NA' as unit_concept_id
 ,'field_plausible_temporal_after.sql' as sql_file
 ,'Plausibility' as category
 ,'Temporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'field_plausibletemporalafter_dose_era_dose_era_start_date' as checkid
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
 'DOSE_ERA.DOSE_ERA_START_DATE' AS violating_field,
 cdmTable.*
 FROM {{ source('omop',  'dose_era' ) }} cdmTable
 JOIN {{ source('omop',  'person' ) }} plausibleTable ON cdmTable.person_id = plausibleTable.person_id
 WHERE
 COALESCE(
 CAST(plausibleTable.BIRTH_DATETIME AS DATE),
 CAST(CONCAT(plausibleTable.year_of_birth,'-06-01') AS DATE)
 )
 > CAST(cdmTable.DOSE_ERA_START_DATE AS DATE)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'dose_era' ) }} cdmTable
) denominator
) cte
)

SELECT *
FROM cte_all
