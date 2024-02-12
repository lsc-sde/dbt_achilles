WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 3027491 (HELICOBACTER PYLORI IGG AB [UNITS/VOLUME] IN SERUM BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027491' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027491' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027491
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027491
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
 ,'The number and percent of records for a given CONCEPT_ID 3027492 (DRY BODY WEIGHT MEASURED) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8504,8504,8564,8564,8576,8576,8739,8739,9275,9275,9294,9294,9295,9295,9319,9319,9343,9343,9345,9345,9347,9347,9354,9354,9356,9356,9357,9357,9372,9372,9373,9373,9374,9374,9392,9392,9400,9400,9402,9402,9409,9409,9410,9410,9425,9425,9479,9479,9485,9485,9496,9496,9529,9529,9600,9600,9647,9647,9648,9648,9655,9655)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027492' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027492' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027492
 AND
 m.unit_concept_id NOT IN (8504,8504,8564,8564,8576,8576,8739,8739,9275,9275,9294,9294,9295,9295,9319,9319,9343,9343,9345,9345,9347,9347,9354,9354,9356,9356,9357,9357,9372,9372,9373,9373,9374,9374,9392,9392,9400,9400,9402,9402,9409,9409,9410,9410,9425,9425,9479,9479,9485,9485,9496,9496,9529,9529,9600,9600,9647,9647,9648,9648,9655,9655)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027492
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
 ,'The number and percent of records for a given CONCEPT_ID 3028799 (HERPES SIMPLEX VIRUS 1 GLYCOPROTEIN G IGG AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3028799' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3028799' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028799
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028799
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
 ,'The number and percent of records for a given CONCEPT_ID 3030675 (APTT W EXCESS HEXAGONAL PHOSPHOLIPID (STACLOT LA CONFIRM)) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3030675' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3030675' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3030675
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3030675
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
 ,'The number and percent of records for a given CONCEPT_ID 3032496 (FIRST AND SECOND TRIMESTER INTEGRATED MATERNAL SCREEN [INTERPRETATION]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3032496' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3032496' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3032496
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3032496
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
 ,'The number and percent of records for a given CONCEPT_ID 3034962 (GLUCOSE [MASS/VOLUME] IN CAPILLARY BLOOD BY GLUCOMETER) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3034962' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3034962' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3034962
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3034962
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
 ,'The number and percent of records for a given CONCEPT_ID 3035899 (CHOLESTEROL IN LDL [MASS/VOLUME] IN SERUM OR PLASMA ULTRACENTRIFUGATE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3035899' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3035899' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3035899
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3035899
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
 ,'The number and percent of records for a given CONCEPT_ID 3038735 (ABO GROUP [TYPE] IN CORD BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3038735' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3038735' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3038735
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3038735
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
 ,'The number and percent of records for a given CONCEPT_ID 3039899 (CLOTTING TIME.EXTRINSIC COAGULATION SYSTEM ACTIVATED.FIBRINOLYSIS SUPPRESSED OF BLOOD BY ROTATIONAL TEG) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3039899' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3039899' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3039899
 AND
 m.unit_concept_id NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3039899
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
 ,CONCAT('The number and percent of records for a given CONCEPT_ID 3041290 (CARBON DIOXIDE [PARTIAL PRESSURE] ADJUSTED TO PATIENT','\047','S ACTUAL TEMPERATURE IN VENOUS BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).') as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3041290' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3041290' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041290
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041290
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
 ,'The number and percent of records for a given CONCEPT_ID 3041423 (SJOGRENS SYNDROME-A EXTRACTABLE NUCLEAR 60KD AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3041423' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3041423' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041423
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041423
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
 ,'The number and percent of records for a given CONCEPT_ID 3043953 (RUBELLA VIRUS IGG AB [PRESENCE] IN SERUM OR PLASMA BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3043953' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3043953' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3043953
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3043953
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
 ,'The number and percent of records for a given CONCEPT_ID 3044009 (APTT.LUPUS SENSITIVE (LA SCREEN)) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3044009' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3044009' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3044009
 AND
 m.unit_concept_id NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3044009
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
 ,'The number and percent of records for a given CONCEPT_ID 3044331 (CALCIUM.IONIZED [MOLES/VOLUME] IN ARTERIAL BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3044331' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3044331' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3044331
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3044331
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
 ,'The number and percent of records for a given CONCEPT_ID 3045178 (PATHOLOGY REPORT FINAL DIAGNOSIS) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3045178' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3045178' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3045178
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3045178
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
 ,'The number and percent of records for a given CONCEPT_ID 3046549 (LIPOPROTEIN.BETA.SUBPARTICLE.SMALL [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3046549' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3046549' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046549
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046549
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
 ,'The number and percent of records for a given CONCEPT_ID 3046870 (TISSUE TRANSGLUTAMINASE IGG AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (9260)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3046870' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3046870' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046870
 AND
 m.unit_concept_id NOT IN (9260)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046870
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
 ,'The number and percent of records for a given CONCEPT_ID 3048905 (PARATHYRIN.INTACT [MASS/VOLUME] IN SERUM OR PLASMA --POST EXCISION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3048905' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3048905' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3048905
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3048905
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
 ,'The number and percent of records for a given CONCEPT_ID 3049389 (GALACTOSEMIAS NEWBORN SCREEN INTERPRETATION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3049389' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3049389' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3049389
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3049389
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
 ,'The number and percent of records for a given CONCEPT_ID 3051969 (BACTERIAL VAGINOSIS AND VAGINITIS RRNA PANEL - VAGINAL FLUID BY PROBE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3051969' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3051969' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3051969
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3051969
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
 ,'The number and percent of records for a given CONCEPT_ID 21493473 (CRYPTOSPORIDIUM SP DNA [PRESENCE] IN STOOL BY NAA WITH NON-PROBE DETECTION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'21493473' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_21493473' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493473
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493473
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
 ,'The number and percent of records for a given CONCEPT_ID 40757503 (CHOLESTEROL IN HDL [MASS/VOLUME] IN BODY FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40757503' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40757503' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40757503
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40757503
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
 ,'The number and percent of records for a given CONCEPT_ID 40760139 (URINALYSIS DIPSTICK W REFLEX MICROSCOPIC PANEL - URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40760139' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40760139' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40760139
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40760139
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
 ,'The number and percent of records for a given CONCEPT_ID 40761551 (BILIRUBIN [PRESENCE] IN URINE BY CONFIRMATORY METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40761551' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40761551' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40761551
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40761551
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
 ,'The number and percent of records for a given CONCEPT_ID 40763487 (DATE OF PREVIOUS PAP SMEAR) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40763487' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40763487' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40763487
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40763487
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
 ,'The number and percent of records for a given CONCEPT_ID 40764186 (NOROXYCODONE [MASS/VOLUME] IN URINE BY CONFIRMATORY METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40764186' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40764186' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40764186
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40764186
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
 ,'The number and percent of records for a given CONCEPT_ID 40768653 (HOW MANY HOURS DO YOU NORMALLY SLEEP [DI-PAD]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40768653' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40768653' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40768653
 AND
 m.unit_concept_id NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40768653
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
 ,'The number and percent of records for a given CONCEPT_ID 42529229 (PROSTATE SPECIFIC AG [MASS/VOLUME] IN SERUM OR PLASMA BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'42529229' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_42529229' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 42529229
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 42529229
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
 ,'The number and percent of records for a given CONCEPT_ID 42869531 (GAMMA INTERFERON BACKGROUND [UNITS/VOLUME] IN BLOOD BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'42869531' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_42869531' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 42869531
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 42869531
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
 ,'The number and percent of records for a given CONCEPT_ID 43055233 (STREPTOCOCCUS PYOGENES EXOTOXIN B SPEB GENE [PRESENCE] IN SPECIMEN BY NAA WITH PROBE DETECTION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'43055233' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_43055233' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 43055233
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 43055233
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
 ,'The number and percent of records for a given CONCEPT_ID 43533393 (OPIATES [PRESENCE] IN URINE BY SCREEN METHOD >300 NG/ML) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'43533393' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_43533393' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 43533393
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 43533393
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
 ,'The number and percent of records for a given CONCEPT_ID 43533776 (REAGIN AND TREPONEMA PALLIDUM IGG AND IGM [INTERPRETATION] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'43533776' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_43533776' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 43533776
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 43533776
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
 ,'The number and percent of records for a given CONCEPT_ID 46234833 (BACTERIA IDENTIFIED IN ABSCESS BY ANAEROBE+AEROBE CULTURE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'46234833' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_46234833' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 46234833
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 46234833
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
 ,'The number and percent of records for a given CONCEPT_ID 3008598 (THYROXINE (T4) FREE [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3008598' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3008598' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3008598
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3008598
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
 ,'The number and percent of records for a given CONCEPT_ID 3009105 (ERYTHROCYTES [#/VOLUME] IN URINE BY TEST STRIP) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3009105' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3009105' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009105
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009105
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
 ,'The number and percent of records for a given CONCEPT_ID 3009583 (CODEINE [MASS/VOLUME] IN URINE BY CONFIRMATORY METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3009583' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3009583' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009583
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009583
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
 ,'The number and percent of records for a given CONCEPT_ID 3009966 (CHOLESTEROL IN LDL [MASS/VOLUME] IN SERUM OR PLASMA BY DIRECT ASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3009966' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3009966' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009966
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009966
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
 ,'The number and percent of records for a given CONCEPT_ID 3010231 (INDIRECT ANTIGLOBULIN TEST.COMPLEMENT SPECIFIC REAGENT [PRESENCE] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010231' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010231' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010231
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010231
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
 ,'The number and percent of records for a given CONCEPT_ID 3010813 (LEUKOCYTES [#/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010813' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010813' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010813
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010813
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
 ,'The number and percent of records for a given CONCEPT_ID 3010854 (NEISSERIA GONORRHOEAE RRNA [PRESENCE] IN SPECIMEN BY PROBE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010854' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010854' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010854
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010854
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
 ,'The number and percent of records for a given CONCEPT_ID 3011960 (NATRIURETIC PEPTIDE B [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3011960' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3011960' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011960
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011960
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
 ,'The number and percent of records for a given CONCEPT_ID 3013115 (EOSINOPHILS [#/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3013115' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3013115' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013115
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013115
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
 ,'The number and percent of records for a given CONCEPT_ID 3013362 (BENZODIAZEPINES TESTED FOR IN URINE BY SCREEN METHOD NOMINAL) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3013362' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3013362' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013362
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013362
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
 ,'The number and percent of records for a given CONCEPT_ID 3013520 (CANCER AG 27-29 [UNITS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9260,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3013520' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3013520' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013520
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9260,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013520
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
 ,'The number and percent of records for a given CONCEPT_ID 3014295 (OXYGEN SATURATION IN ARTERIAL CORD BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3014295' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3014295' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014295
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014295
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
 ,'The number and percent of records for a given CONCEPT_ID 3014716 (GLUCOSE [MASS/VOLUME] IN SERUM OR PLASMA --1 HOUR POST 100 G GLUCOSE PO) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3014716' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3014716' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014716
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014716
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
 ,'The number and percent of records for a given CONCEPT_ID 3014923 (NORTRAMADOL [MASS/VOLUME] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3014923' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3014923' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014923
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014923
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
 ,'The number and percent of records for a given CONCEPT_ID 3015586 (SEGMENTED NEUTROPHILS [#/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015586' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015586' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015586
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015586
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
 ,'The number and percent of records for a given CONCEPT_ID 3015632 (CARBON DIOXIDE, TOTAL [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015632' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015632' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015632
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015632
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
 ,'The number and percent of records for a given CONCEPT_ID 3015681 (EPSTEIN BARR VIRUS CAPSID IGG AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015681' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015681' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015681
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015681
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte
)

SELECT *
FROM cte_all
