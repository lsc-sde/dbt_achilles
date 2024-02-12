WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleUnitConceptIds' as check_name
 ,'CONCEPT' as check_level
 ,'The number and percent of records for a given CONCEPT_ID 46235690 (LUPUS ANTICOAGULANT THREE SCREENING TESTS W REFLEX [INTERPRETATION]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'46235690' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_46235690' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 46235690
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 46235690
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
 ,'The number and percent of records for a given CONCEPT_ID 3002516 (MICROSCOPIC OBSERVATION [IDENTIFIER] IN SPUTUM BY GRAM STAIN) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3002516' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3002516' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002516
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3002516
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
 ,'The number and percent of records for a given CONCEPT_ID 3003645 (BORRELIA BURGDORFERI AB [UNITS/VOLUME] IN SERUM BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3003645' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3003645' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003645
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3003645
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
 ,'The number and percent of records for a given CONCEPT_ID 3004206 (CELLS COUNTED TOTAL [#] IN CEREBRAL SPINAL FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3004206' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3004206' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004206
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004206
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
 ,'The number and percent of records for a given CONCEPT_ID 3004248 (SEX HORMONE BINDING GLOBULIN [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3004248' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3004248' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004248
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3004248
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
 ,'The number and percent of records for a given CONCEPT_ID 3005241 (LINEZOLID [SUSCEPTIBILITY] BY MINIMUM INHIBITORY CONCENTRATION (MIC)) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3005241' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3005241' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005241
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005241
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
 ,'The number and percent of records for a given CONCEPT_ID 3005770 (CREATININE RENAL CLEARANCE IN 24 HOUR URINE AND SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8698,8795,8857,8930,32700,44777523,44777603,44777613,44777614,44777664)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3005770' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3005770' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005770
 AND
 m.unit_concept_id NOT IN (8698,8795,8857,8930,32700,44777523,44777603,44777613,44777614,44777664)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3005770
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
 ,'The number and percent of records for a given CONCEPT_ID 3006093 (CHLAMYDIA TRACHOMATIS DNA [PRESENCE] IN SPECIMEN BY NAA WITH PROBE DETECTION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3006093' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3006093' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006093
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006093
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
 ,'The number and percent of records for a given CONCEPT_ID 3006433 (CRYPTOSPORIDIUM SP AG [PRESENCE] IN STOOL) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3006433' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3006433' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006433
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006433
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
 ,'The number and percent of records for a given CONCEPT_ID 3006504 (EOSINOPHILS/100 LEUKOCYTES IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3006504' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3006504' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006504
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006504
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
 ,'The number and percent of records for a given CONCEPT_ID 3006508 (ARSENIC/CREATININE [MASS RATIO] IN 24 HOUR URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (9014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3006508' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3006508' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006508
 AND
 m.unit_concept_id NOT IN (9014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3006508
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
 ,'The number and percent of records for a given CONCEPT_ID 3007070 (CHOLESTEROL IN HDL [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007070' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007070' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007070
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007070
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
 ,'The number and percent of records for a given CONCEPT_ID 3007424 (ALBUMIN/PROTEIN.TOTAL IN SERUM OR PLASMA BY ELECTROPHORESIS) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3007424' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3007424' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007424
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3007424
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
 ,'The number and percent of records for a given CONCEPT_ID 3010072 (ALBUMIN CONCENTRATION GIVEN) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010072' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010072' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010072
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010072
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
 ,'The number and percent of records for a given CONCEPT_ID 3010140 (CARBON DIOXIDE, TOTAL [MOLES/VOLUME] IN VENOUS BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010140' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010140' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010140
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010140
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
 ,'The number and percent of records for a given CONCEPT_ID 3010873 (BILIRUBIN.TOTAL [MASS/VOLUME] IN URINE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3010873' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3010873' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010873
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3010873
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
 ,'The number and percent of records for a given CONCEPT_ID 3011380 (VANCOMYCIN [MASS/VOLUME] IN SERUM OR PLASMA --RANDOM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3011380' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3011380' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011380
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011380
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
 ,'The number and percent of records for a given CONCEPT_ID 3011412 (CD3 CELLS [#/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3011412' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3011412' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011412
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3011412
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
 ,'The number and percent of records for a given CONCEPT_ID 3012671 (MONOCYTES+MACROPHAGES/100 LEUKOCYTES IN BODY FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3012671' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3012671' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3012671
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3012671
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
 ,'The number and percent of records for a given CONCEPT_ID 3012932 (HAZELNUT IGE AB [UNITS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3012932' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3012932' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3012932
 AND
 m.unit_concept_id NOT IN (8645,8719,8750,8763,8810,8860,8923,8924,8985,9040,9058,9093,9332,9525,9550,44777568,44777578,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3012932
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
 ,'The number and percent of records for a given CONCEPT_ID 3016324 (MINOCYCLINE [SUSCEPTIBILITY]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3016324' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3016324' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016324
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016324
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
 ,'The number and percent of records for a given CONCEPT_ID 3016723 (CREATININE [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3016723' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3016723' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016723
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3016723
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
 ,'The number and percent of records for a given CONCEPT_ID 3018199 (BAND FORM NEUTROPHILS [#/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3018199' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3018199' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3018199
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3018199
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
 ,'The number and percent of records for a given CONCEPT_ID 3019174 (DRVVT (LA SCREEN)) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3019174' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3019174' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019174
 AND
 m.unit_concept_id NOT IN (8505,8511,8512,8550,8555,9399,9448,9449,9450,9451,9537,9580,9581,9582,9583,9592,9593,9616,9634,9676,32960,32961,44777661)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019174
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
 ,'The number and percent of records for a given CONCEPT_ID 3019225 (PH OF SPECIMEN) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8482,8518)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3019225' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3019225' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019225
 AND
 m.unit_concept_id NOT IN (8482,8518)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019225
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
 ,'The number and percent of records for a given CONCEPT_ID 3019510 (HEPATITIS B VIRUS SURFACE AG [PRESENCE] IN SERUM OR PLASMA BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3019510' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3019510' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019510
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019510
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
 ,'The number and percent of records for a given CONCEPT_ID 3019550 (SODIUM [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3019550' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3019550' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019550
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3019550
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
 ,'The number and percent of records for a given CONCEPT_ID 3021440 (PROMYELOCYTES/100 LEUKOCYTES IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3021440' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3021440' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021440
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3021440
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
 ,'The number and percent of records for a given CONCEPT_ID 3022231 (EOSINOPHILS/100 LEUKOCYTES IN BODY FLUID BY MANUAL COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8554)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3022231' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3022231' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3022231
 AND
 m.unit_concept_id NOT IN (8554)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3022231
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
 ,'The number and percent of records for a given CONCEPT_ID 3025926 (BODY TEMPERATURE - CORE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (9289,9289,9523,9523,586323,586323)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3025926' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3025926' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025926
 AND
 m.unit_concept_id NOT IN (9289,9289,9523,9523,586323,586323)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3025926
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
 ,'The number and percent of records for a given CONCEPT_ID 3027005 (BACTERIA IDENTIFIED IN TISSUE BY AEROBE CULTURE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3027005' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3027005' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027005
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3027005
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
 ,'The number and percent of records for a given CONCEPT_ID 3028192 (IGG SUBCLASS 2 [MASS/VOLUME] IN SERUM) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3028192' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3028192' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028192
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3028192
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
 ,'The number and percent of records for a given CONCEPT_ID 3029080 (HEMOGLOBIN [ENTITIC MASS] IN RETICULOCYTES) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3029080' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3029080' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3029080
 AND
 m.unit_concept_id NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3029080
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
 ,'The number and percent of records for a given CONCEPT_ID 3029162 (EPITHELIAL CELLS.SQUAMOUS [#/AREA] IN URINE SEDIMENT BY AUTOMATED COUNT) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8786)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3029162' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3029162' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3029162
 AND
 m.unit_concept_id NOT IN (8786)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3029162
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
 ,'The number and percent of records for a given CONCEPT_ID 3030703 (EVEROLIMUS [MASS/VOLUME] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3030703' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3030703' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3030703
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3030703
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
 ,'The number and percent of records for a given CONCEPT_ID 3030839 (AMMONIA [MASS OR MOLES/VOLUME] IN PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3030839' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3030839' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3030839
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3030839
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
 ,'The number and percent of records for a given CONCEPT_ID 3032155 (METANEPHRINE FREE [MOLES/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3032155' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3032155' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3032155
 AND
 m.unit_concept_id NOT IN (8729,8736,8745,8749,8753,8839,8843,8875,9440,9490,9491,9501,9553,9557,9559,9575,9586,9587,9588,9591,9608,9621,9631,9632,9654,9673,45891014)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3032155
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
 ,'The number and percent of records for a given CONCEPT_ID 3038136 (CHORIOGONADOTROPIN.BETA SUBUNIT [UNITS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8645,8645,8719,8719,8750,8750,8763,8763,8810,8810,8860,8860,8923,8923,8924,8924,8985,8985,9040,9040,9058,9058,9093,9093,9332,9332,9525,9525,9550,9550,44777568,44777568,44777578,44777578,44777583,44777583)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3038136' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3038136' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3038136
 AND
 m.unit_concept_id NOT IN (8645,8645,8719,8719,8750,8750,8763,8763,8810,8810,8860,8860,8923,8923,8924,8924,8985,8985,9040,9040,9058,9058,9093,9093,9332,9332,9525,9525,9550,9550,44777568,44777568,44777578,44777578,44777583,44777583)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3038136
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
 ,'The number and percent of records for a given CONCEPT_ID 3041366 (CRYSTALS [PRESENCE] IN URINE BY AUTOMATED) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3041366' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3041366' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041366
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3041366
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
 ,'The number and percent of records for a given CONCEPT_ID 3045870 (BLOOD PRODUCT UNITS REQUESTED [#]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3045870' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3045870' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3045870
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3045870
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
 ,'The number and percent of records for a given CONCEPT_ID 3046171 (BETA 2 GLYCOPROTEIN 1 IGM AB [UNITS/VOLUME] IN SERUM OR PLASMA BY IMMUNOASSAY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3046171' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3046171' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046171
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046171
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
 ,'The number and percent of records for a given CONCEPT_ID 3046321 (NEUTROPHILS [#/VOLUME] IN BODY FLUID) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3046321' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3046321' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046321
 AND
 m.unit_concept_id NOT IN (8647,8695,8712,8734,8784,8785,8799,8815,8816,8829,8848,8888,8931,8938,8961,8980,9156,9157,9158,9245,9254,9257,9423,9426,9435,9436,9442,9444,9445,9446,32706,44777520,44777558,44777561,44777562,44777569,44777575,44777580,44777588,45891003)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3046321
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
 ,'The number and percent of records for a given CONCEPT_ID 3048689 (CALPROTECTIN [MASS/MASS] IN STOOL) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8720)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3048689' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3048689' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3048689
 AND
 m.unit_concept_id NOT IN (8720)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3048689
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
 ,'The number and percent of records for a given CONCEPT_ID 3049536 (25-HYDROXYVITAMIN D2 [MASS/VOLUME] IN SERUM OR PLASMA) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3049536' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3049536' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3049536
 AND
 m.unit_concept_id NOT IN (8636,8713,8725,8748,8751,8817,8820,8837,8840,8842,8845,8859,8861,8950,9028,9503,9514,9530,9532,9560,9564,9625,32964,32965,44777535,44777592,44777638,45956701)
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3049536
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
 ,'The number and percent of records for a given CONCEPT_ID 3050169 (ERYTHROCYTE INCLUSION BODIES [IDENTIFIER] IN BLOOD) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,' 3050169' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3050169' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 3050169
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 3050169
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
 ,'The number and percent of records for a given CONCEPT_ID 21493476 (GIARDIA LAMBLIA DNA [PRESENCE] IN STOOL BY NAA WITH NON-PROBE DETECTION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'21493476' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_21493476' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493476
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493476
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
 ,'The number and percent of records for a given CONCEPT_ID 21493480 (ROTAVIRUS A RNA [PRESENCE] IN STOOL BY NAA WITH NON-PROBE DETECTION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'21493480' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_21493480' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493480
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 21493480
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
 ,'The number and percent of records for a given CONCEPT_ID 40760845 (PROTEIN [PRESENCE] IN URINE BY AUTOMATED TEST STRIP) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40760845' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40760845' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40760845
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40760845
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
 ,'The number and percent of records for a given CONCEPT_ID 40763395 (CRYSTALS.AMORPHOUS [PRESENCE] IN URINE SEDIMENT BY LIGHT MICROSCOPY) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40763395' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40763395' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40763395
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40763395
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
 ,'The number and percent of records for a given CONCEPT_ID 40766642 (ARE YOU CONSIDERING QUITTING SMOKING DURING THE NEXT 6 MONTHS [PLCO]) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN (NA)).' as check_description
 ,'MEASUREMENT' as cdm_table_name
 ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
 ,'40766642' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_unit_concept_ids.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Verification' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40766642' as checkid
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
 WHERE m.MEASUREMENT_CONCEPT_ID = 40766642
 AND
 m.unit_concept_id IS NOT NULL
 AND m.value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.MEASUREMENT m
 WHERE m.MEASUREMENT_CONCEPT_ID = 40766642
 AND value_as_number IS NOT NULL
 AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte
)

SELECT *
FROM cte_all
