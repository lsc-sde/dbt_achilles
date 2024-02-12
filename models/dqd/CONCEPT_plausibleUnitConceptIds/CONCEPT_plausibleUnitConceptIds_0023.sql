WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 42869419 (ECG NEMSIS) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'42869419' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_42869419' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 42869419
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 42869419
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
 ,'The number and percent of records for a given CONCEPT_ID 3000053 (HEAD OCCIPITAL-FRONTAL CIRCUMFERENCE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8577,8582,8588,9279,9280,9281,9282,9290,9305,9306,9307,9308,9309,9310,9311,9321,9326,9327,9330,9349,9350,9351,9352,9355,9361,9362,9363,9364,9365,9370,9371,9375,9376,9377,9381,9384,9385,9386,9395,9396,9397,9398,9407,9419,9420,9421,9487,9497,9536,9546,9624,9629,9666,32739,32963)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3000053' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3000053' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3000053
 AND
 m.unit_concept_id NOT IN (8577,8582,8588,9279,9280,9281,9282,9290,9305,9306,9307,9308,9309,9310,9311,9321,9326,9327,9330,9349,9350,9351,9352,9355,9361,9362,9363,9364,9365,9370,9371,9375,9376,9377,9381,9384,9385,9386,9395,9396,9397,9398,9407,9419,9420,9421,9487,9497,9536,9546,9624,9629,9666,32739,32963)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3000053
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
 ,'The number and percent of records for a given CONCEPT_ID 3000461 (PRESSURE SUPPORT SETTING VENTILATOR) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3000461' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3000461' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3000461
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3000461
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
 ,'The number and percent of records for a given CONCEPT_ID 3001008 (EPITHELIAL CELLS.SQUAMOUS [#/AREA] IN URINE SEDIMENT BY MICROSCOPY HIGH POWER FIELD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8786)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3001008' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3001008' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3001008
 AND
 m.unit_concept_id NOT IN (8786)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3001008
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
 ,'The number and percent of records for a given CONCEPT_ID 3001949 (TACROLIMUS [MASS] OF DOSE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3001949' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3001949' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3001949
 AND
 m.unit_concept_id NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3001949
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
 ,'The number and percent of records for a given CONCEPT_ID 3002052 (WBC CASTS [#/AREA] IN URINE SEDIMENT BY MICROSCOPY LOW POWER FIELD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8765)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3002052' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3002052' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002052
 AND
 m.unit_concept_id NOT IN (8765)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002052
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
 ,'The number and percent of records for a given CONCEPT_ID 3002148 (METHYLENEDIOXYMETHAMPHETAMINE [PRESENCE] IN URINE BY SCREEN METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3002148' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3002148' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002148
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002148
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
 ,'The number and percent of records for a given CONCEPT_ID 3002620 (HOWELL-JOLLY BODIES [PRESENCE] IN BLOOD BY LIGHT MICROSCOPY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3002620' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3002620' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002620
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002620
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
 ,'The number and percent of records for a given CONCEPT_ID 3003214 (PLATELET MORPHOLOGY FINDING [IDENTIFIER] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3003214' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3003214' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003214
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003214
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
 ,'The number and percent of records for a given CONCEPT_ID 3003291 (CASTS [PRESENCE] IN URINE SEDIMENT BY LIGHT MICROSCOPY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3003291' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3003291' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003291
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003291
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
 ,'The number and percent of records for a given CONCEPT_ID 3003470 (HEPATITIS C VIRUS AB [UNITS/VOLUME] IN SERUM BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3003470' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3003470' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003470
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003470
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
 ,'The number and percent of records for a given CONCEPT_ID 3004501 (GLUCOSE [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3004501' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3004501' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004501
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004501
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
 ,'The number and percent of records for a given CONCEPT_ID 3004562 (BACTERIA [PRESENCE] IN URINE SEDIMENT BY LIGHT MICROSCOPY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3004562' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3004562' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004562
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004562
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
 ,'The number and percent of records for a given CONCEPT_ID 3005031 (MICROALBUMIN [MASS/VOLUME] IN 24 HOUR URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8636,8713,8713,8725,8725,8748,8748,8751,8751,8817,8817,8820,8820,8837,8837,8840,8840,8842,8842,8845,8845,8859,8859,8861,8861,8950,8950,9028,9028,9503,9503,9514,9514,9530,9530,9532,9532,9560,9560,9564,9564,9625,9625,32964,32964,32965,32965,44777535,44777535,44777592,44777592,44777638,44777638,45956701,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3005031' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3005031' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005031
 AND
 m.unit_concept_id NOT IN (8636,8636,8713,8713,8725,8725,8748,8748,8751,8751,8817,8817,8820,8820,8837,8837,8840,8840,8842,8842,8845,8845,8859,8859,8861,8861,8950,8950,9028,9028,9503,9503,9514,9514,9530,9530,9532,9532,9560,9560,9564,9564,9625,9625,32964,32964,32965,32965,44777535,44777535,44777592,44777592,44777638,44777638,45956701,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005031
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
 ,'The number and percent of records for a given CONCEPT_ID 3005081 (HEMOGLOBIN S/HEMOGLOBIN.TOTAL IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3005081' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3005081' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005081
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005081
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
 ,'The number and percent of records for a given CONCEPT_ID 3005658 (CASTS [#/AREA] IN URINE SEDIMENT BY MICROSCOPY LOW POWER FIELD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8765)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3005658' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3005658' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005658
 AND
 m.unit_concept_id NOT IN (8765)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005658
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
 ,'The number and percent of records for a given CONCEPT_ID 3005833 (GAS PANEL - BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3005833' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3005833' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005833
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005833
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
 ,'The number and percent of records for a given CONCEPT_ID 3006545 (APPEARANCE OF STONE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3006545' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3006545' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006545
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006545
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
 ,'The number and percent of records for a given CONCEPT_ID 3007238 (NUCLEATED ERYTHROCYTES [#/VOLUME] IN BLOOD BY AUTOMATED COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007238' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007238' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007238
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007238
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
 ,'The number and percent of records for a given CONCEPT_ID 3007696 (CARBON DIOXIDE [PARTIAL PRESSURE] IN VENOUS CORD BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007696' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007696' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007696
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007696
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
 ,'The number and percent of records for a given CONCEPT_ID 3007867 (HEPATITIS 1996 PANEL - SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007867' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007867' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007867
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007867
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
 ,'The number and percent of records for a given CONCEPT_ID 3008122 (PATHOLOGY STUDY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3008122' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3008122' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3008122
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3008122
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
 ,'The number and percent of records for a given CONCEPT_ID 3009306 (ALPHA-1-FETOPROTEIN [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3009306' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3009306' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009306
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3009306
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
 ,'The number and percent of records for a given CONCEPT_ID 3011681 (PLASMA CELLS/100 LEUKOCYTES IN BODY FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3011681' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3011681' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011681
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011681
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
 ,'The number and percent of records for a given CONCEPT_ID 3011802 (PROPOXYPHENE [PRESENCE] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3011802' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3011802' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011802
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011802
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
 ,'The number and percent of records for a given CONCEPT_ID 3011988 (PH OF CORD BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8482,8518)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3011988' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3011988' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011988
 AND
 m.unit_concept_id NOT IN (8482,8518)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011988
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
 ,'The number and percent of records for a given CONCEPT_ID 3012592 (PHENCYCLIDINE [PRESENCE] IN URINE BY SCREEN METHOD >25 NG/ML) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3012592' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3012592' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3012592
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3012592
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
 ,CONCAT('The number and percent of records for a given CONCEPT_ID 3013702 (OXYGEN [PARTIAL PRESSURE] ADJUSTED TO PATIENT','\047','S ACTUAL TEMPERATURE IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).') as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3013702' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3013702' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013702
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013702
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
 ,'The number and percent of records for a given CONCEPT_ID 3013721 (ASPARTATE AMINOTRANSFERASE [ENZYMATIC ACTIVITY/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3013721' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3013721' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013721
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3013721
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
 ,'The number and percent of records for a given CONCEPT_ID 3014053 (GLUCOSE [MASS/VOLUME] IN BLOOD BY TEST STRIP MANUAL) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3014053' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3014053' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014053
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3014053
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
 ,'The number and percent of records for a given CONCEPT_ID 3015015 (HIV 2 AB [PRESENCE] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015015' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015015' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015015
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015015
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
 ,'The number and percent of records for a given CONCEPT_ID 3015546 (ASPARTATE [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015546' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015546' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015546
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015546
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
 ,'The number and percent of records for a given CONCEPT_ID 3015701 (ILLNESS OR INJURY ONSET DATE AND TIME) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3015701' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015701' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015701
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3015701
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
 ,'The number and percent of records for a given CONCEPT_ID 3017509 (PHENCYCLIDINE [PRESENCE] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3017509' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3017509' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3017509
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3017509
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
 ,'The number and percent of records for a given CONCEPT_ID 3018171 (CHORIOGONADOTROPIN [UNITS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3018171' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3018171' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3018171
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3018171
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
 ,'The number and percent of records for a given CONCEPT_ID 3018336 (MEAN BLOOD PRESSURE--SUPINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3018336' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3018336' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3018336
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3018336
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
 ,'The number and percent of records for a given CONCEPT_ID 3020650 (GLUCOSE [PRESENCE] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3020650' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3020650' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3020650
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3020650
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
 ,'The number and percent of records for a given CONCEPT_ID 3021888 (DIRECT ANTIGLOBULIN TEST.IGG SPECIFIC REAGENT [INTERPRETATION] ON RED BLOOD CELLS) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3021888' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3021888' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021888
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021888
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
 ,'The number and percent of records for a given CONCEPT_ID 3022304 (AGE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3022304' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3022304' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3022304
 AND
 m.unit_concept_id NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3022304
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
 ,'The number and percent of records for a given CONCEPT_ID 3023150 (MYELOPEROXIDASE AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3023150' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3023150' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023150
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3023150
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
 ,'The number and percent of records for a given CONCEPT_ID 3024263 (APPEARANCE OF SYNOVIAL FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3024263' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3024263' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3024263
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3024263
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
 ,'The number and percent of records for a given CONCEPT_ID 3024386 (PLATELET MEAN VOLUME [ENTITIC VOLUME] IN BLOOD BY REES-ECKER) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8519,8583,8587,8686,9261,9263,9271,9277,9283,9285,9286,9287,9288,9292,9293,9296,9300,9301,9303,9304,9314,9316,9317,9318,9366,9367,9382,9383,9390,9391,9393,9394,9412,9416,9482,9486,9515,9520,9535,9606,9628,9643,9665,44777531,44777662)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3024386' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3024386' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3024386
 AND
 m.unit_concept_id NOT IN (8519,8583,8587,8686,9261,9263,9271,9277,9283,9285,9286,9287,9288,9292,9293,9296,9300,9301,9303,9304,9314,9316,9317,9318,9366,9367,9382,9383,9390,9391,9393,9394,9412,9416,9482,9486,9515,9520,9535,9606,9628,9643,9665,44777531,44777662)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3024386
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
 ,'The number and percent of records for a given CONCEPT_ID 3025665 (MAGNESIUM [MOLES/VOLUME] IN SPECIMEN) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3025665' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3025665' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025665
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025665
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
 ,'The number and percent of records for a given CONCEPT_ID 3026417 (BUTALBITAL [MASS/VOLUME] IN URINE BY CONFIRMATORY METHOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3026417' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3026417' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3026417
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3026417
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
 ,'The number and percent of records for a given CONCEPT_ID 3026652 (MITOCHONDRIA AB [TITER] IN SERUM BY IMMUNOFLUORESCENCE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8525)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3026652' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3026652' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3026652
 AND
 m.unit_concept_id NOT IN (8525)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3026652
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
 ,'The number and percent of records for a given CONCEPT_ID 3027270 (SEGMENTED NEUTROPHILS [#/VOLUME] IN BLOOD BY MANUAL COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027270' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027270' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027270
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027270
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
 ,'The number and percent of records for a given CONCEPT_ID 3027315 (OXYGEN [PARTIAL PRESSURE] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027315' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027315' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027315
 AND
 m.unit_concept_id NOT IN (8876,9328,9329,9389,9454,9455,9464,9547,9548,9623,44777527,44777590,44777602)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027315
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
 ,'The number and percent of records for a given CONCEPT_ID 3027572 (XANTHOCHROMIA [PRESENCE] OF CEREBRAL SPINAL FLUID QUALITATIVE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027572' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027572' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027572
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027572
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
 ,'The number and percent of records for a given CONCEPT_ID 3027944 (AMPHETAMINES [PRESENCE] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027944' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027944' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027944
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027944
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
 ,'The number and percent of records for a given CONCEPT_ID 3027953 (ALDOLASE [ENZYMATIC ACTIVITY/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8645,8719,8719,8750,8750,8763,8763,8810,8810,8860,8860,8923,8923,8924,8924,8985,8985,9040,9040,9058,9058,9093,9093,9332,9332,9525,9525,9550,9550,44777568,44777568,44777578,44777578,44777583,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027953' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027953' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027953
 AND
 m.unit_concept_id NOT IN (8645,8645,8719,8719,8750,8750,8763,8763,8810,8810,8860,8860,8923,8923,8924,8924,8985,8985,9040,9040,9058,9058,9093,9093,9332,9332,9525,9525,9550,9550,44777568,44777568,44777578,44777578,44777583,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027953
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte
)

SELECT *
FROM cte_all
