
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 42869451 (HEMOGLOBIN [ENTITIC MASS] IN RETICULOCYTES BY AUTOMATED COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'42869451' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_42869451' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,NULL as not_applicable_reason
 ,5 as threshold_value
 ,NULL as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
 WHEN denominator.num_rows = 0 THEN 0
 ELSE 1.0*num_violated_rows/denominator.num_rows
 END AS pct_violated_rows,
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT
 COUNT(*) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT
 m.*
 FROM {{ source('omop',  'measurement' ) }} m
 WHERE m.MEASUREMENT_CONCEPT_ID = 42869451
 AND
 m.unit_concept_id NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM {{ source('omop',  'measurement' ) }} m
 WHERE m.MEASUREMENT_CONCEPT_ID = 42869451
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte
)

SELECT *
FROM cte_all
