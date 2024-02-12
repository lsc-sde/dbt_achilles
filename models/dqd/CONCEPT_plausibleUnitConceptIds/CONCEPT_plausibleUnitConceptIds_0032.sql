WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3015076 (SOYBEAN IGE AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015076' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015076' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015076
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015076
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3015697 (HEMOGLOBIN PATTERN [INTERPRETATION] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015697' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015697' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015697
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015697
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3016293 (BICARBONATE [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3016293' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3016293' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016293
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016293
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3016502 (OXYGEN SATURATION IN ARTERIAL BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3016502' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3016502' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016502
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016502
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3016662 (CREATININE [MASS/VOLUME] IN BODY FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3016662' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3016662' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016662
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016662
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3019416 (ACANTHOCYTES [PRESENCE] IN BLOOD BY LIGHT MICROSCOPY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3019416' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3019416' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019416
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019416
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3020149 (25-HYDROXYVITAMIN D3 [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3020149' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3020149' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3020149
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3020149
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3020389 (OVA AND PARASITES IDENTIFIED IN STOOL BY CONCENTRATION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3020389' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3020389' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3020389
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3020389
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3021016 (OXYCODONE [PRESENCE] IN URINE BY SCREEN METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3021016' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3021016' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021016
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021016
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3021226 (SHRIMP IGE AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3021226' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3021226' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021226
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021226
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3023351 (EUROPEAN HOUSE DUST MITE IGE AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3023351' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3023351' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023351
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023351
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3023430 (CAT DANDER IGE AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3023430' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3023430' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023430
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023430
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3023854 (NORDIAZEPAM [MASS/VOLUME] IN URINE BY CONFIRMATORY METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3023854' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3023854' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023854
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023854
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3024139 (IGG SUBCLASS 3 [MASS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3024139' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3024139' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3024139
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3024139
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3025233 (BACTERIA IDENTIFIED IN SPUTUM BY AEROBE CULTURE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3025233' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3025233' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025233
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025233
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3025909 (CREATINE KINASE.MB/CREATINE KINASE.TOTAL IN SERUM OR PLASMA BY CALCULATION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3025909' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3025909' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025909
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025909
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3026250 (TACROLIMUS [MASS/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3026250' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3026250' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3026250
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3026250
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3027598 (MEAN BLOOD PRESSURE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027598' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027598' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027598
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027598
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3028707 (METHADONE [PRESENCE] IN URINE BY SCREEN METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3028707' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3028707' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028707
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028707
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3028741 (6-MONOACETYLMORPHINE (6-MAM) [PRESENCE] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3028741' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3028741' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028741
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028741
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3031266 (GLUCOSE [MASS/VOLUME] IN ARTERIAL BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3031266' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3031266' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3031266
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3031266
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3032250 (DNA DOUBLE STRAND AB [UNITS/VOLUME] IN SERUM BY RADIOIMMUNOASSAY (RIA)) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3032250' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3032250' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3032250
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3032250
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3035715 (GRANULOCYTES [#/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3035715' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3035715' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3035715
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3035715
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3036472 (BLOOD GROUP ANTIBODIES IDENTIFIED IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3036472' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3036472' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3036472
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3036472
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3037061 (DAPTOMYCIN [SUSCEPTIBILITY] BY MINIMUM INHIBITORY CONCENTRATION (MIC)) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3037061' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3037061' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3037061
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3037061
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3039162 (CENTROMERE PROTEIN B AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9260,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3039162' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3039162' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3039162
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9260,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3039162
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3039827 (PLATELETS [#/VOLUME] IN BODY FLUID BY AUTOMATED COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3039827' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3039827' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3039827
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3039827
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3040084 (EPIDERMAL GROWTH FACTOR RECEPTOR AG [PRESENCE] IN TISSUE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3040084' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3040084' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3040084
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3040084
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3041354 (POTASSIUM [MOLES/VOLUME] IN VENOUS BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3041354' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3041354' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041354
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041354
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3041357 (CRYSTALS.AMORPHOUS [#/VOLUME] IN URINE BY COMPUTER ASSISTED METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3041357' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3041357' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041357
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041357
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3043814 (BACTERIA IDENTIFIED IN WOUND DEEP BY CULTURE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3043814' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3043814' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3043814
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3043814
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3044927 (PROTEIN ELECTROPHORESIS PANEL - URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3044927' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3044927' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3044927
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3044927
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3045462 (PROTEIN/CREATININE [RATIO] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (9533,32408,32702,44777595,44777612)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3045462' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3045462' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3045462
 AND
 m.unit_concept_id NOT IN (9533,32408,32702,44777595,44777612)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3045462
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3046412 (ASPARTATE AMINOTRANSFERASE [PRESENCE] IN BODY FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3046412' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3046412' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046412
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046412
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3048541 (NEURAL TUBE DEFECT RISK [LIKELIHOOD] IN FETUS) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3048541' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3048541' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3048541
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3048541
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 21491096 (HEDIS 2016-2018 VALUE SET - PHQ-9 TOTAL SCORE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'21491096' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_21491096' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21491096
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21491096
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 21492989 (INFLUENZA VIRUS B AG [PRESENCE] IN NASOPHARYNX BY RAPID IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'21492989' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_21492989' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21492989
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21492989
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 21493478 (ASTROVIRUS SUBTYPES 1-8 RNA [PRESENCE] IN STOOL BY NAA WITH NON-PROBE DETECTION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'21493478' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_21493478' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493478
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493478
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 40762677 (ALPRAZOLAM [MASS/VOLUME] IN URINE BY CONFIRMATORY METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40762677' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40762677' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40762677
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40762677
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 40763949 (CLINDAMYCIN.INDUCED [SUSCEPTIBILITY]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40763949' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40763949' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40763949
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40763949
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 43533584 (INSULIN-LIKE GROWTH FACTOR-I [Z-SCORE] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'43533584' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_43533584' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 43533584
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 43533584
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 46235605 (DURING THE PAST 4 WEEKS, HOW WOULD YOU RATE YOUR HEALTH IN GENERAL [COOP]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'46235605' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_46235605' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 46235605
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 46235605
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3007124 (RETICULOCYTES/100 ERYTHROCYTES IN BLOOD BY AUTOMATED COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007124' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007124' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007124
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007124
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3007191 (AGE - REPORTED) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007191' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007191' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007191
 AND
 m.unit_concept_id NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007191
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3007220 (CREATINE KINASE [ENZYMATIC ACTIVITY/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007220' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007220' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007220
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007220
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3007302 (HEMOGLOBIN [MASS/VOLUME] IN MIXED VENOUS BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007302' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007302' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007302
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007302
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3007449 (CD3+CD8+ (T8 SUPPRESSOR CELLS) CELLS/100 CELLS IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007449' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007449' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007449
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007449
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3009417 (CHORIOGONADOTROPIN.BETA SUBUNIT (PREGNANCY TEST) [PRESENCE] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3009417' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3009417' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009417
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009417
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3010421 (PH OF BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8482,8518)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010421' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010421' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010421
 AND
 m.unit_concept_id NOT IN (8482,8518)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010421
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3010457 (EOSINOPHILS/100 LEUKOCYTES IN BLOOD BY AUTOMATED COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010457' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010457' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
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
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010457
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010457
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte
)

SELECT *
FROM cte_all
