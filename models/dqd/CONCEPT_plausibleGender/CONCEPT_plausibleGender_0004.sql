WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2617204 (CERVICAL OR VAGINAL CANCER SCREENING, PELVIC AND CLINICAL BREAST EXAMINATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2617204' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2617204' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2617204
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2617204
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2721063 (ANNUAL GYNECOLOGICAL EXAMINATION, NEW PATIENT), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2721063' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2721063' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2721063
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2721063
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2721064 (ANNUAL GYNECOLOGICAL EXAMINATION, ESTABLISHED PATIENT), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2721064' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2721064' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2721064
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2721064
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2780478 (RESECTION OF PROSTATE, PERCUTANEOUS ENDOSCOPIC APPROACH), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2780478' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2780478' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2780478
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2780478
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2780523 (RESECTION OF PREPUCE, EXTERNAL APPROACH), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2780523' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2780523' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2780523
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2780523
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4005743 (FEMALE STERILITY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4005743' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4005743' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4005743
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4005743
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4005933 (HYPOSPADIAS, PENILE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4005933' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4005933' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4005933
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4005933
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4012343 (VAGINAL DISCHARGE SYMPTOM), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4012343' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4012343' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4012343
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4012343
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4016155 (PROSTATISM), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4016155' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4016155' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4016155
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4016155
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4021531 (TOTAL ABDOMINAL HYSTERECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4021531' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4021531' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4021531
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4021531
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4032594 (INFLAMMATION OF SCROTUM), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4032594' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4032594' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4032594
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4032594
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4032622 (LAPAROSCOPIC SUPRACERVICAL HYSTERECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4032622' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4032622' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4032622
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4032622
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4038747 (OBSTETRIC EXAMINATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4038747' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4038747' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4038747
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4038747
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4048225 (NEOPLASM OF ENDOMETRIUM), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4048225' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4048225' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4048225
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4048225
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4050091 (OPEN WOUND OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4050091' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4050091' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4050091
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4050091
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4051956 (VULVOVAGINAL DISEASE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4051956' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4051956' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4051956
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4051956
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4052532 (HYSTEROSCOPY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4052532' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4052532' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4052532
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4052532
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4054550 (OPEN WOUND OF SCROTUM AND TESTES), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4054550' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4054550' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4054550
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4054550
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4056903 (VAGINITIS ASSOCIATED WITH ANOTHER DISORDER), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4056903' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4056903' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4056903
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4056903
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4058792 (DOUCHE OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4058792' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4058792' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4058792
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4058792
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4060207 (VULVAL IRRITATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4060207' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4060207' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4060207
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4060207
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4060556 (UTERINE SCAR FROM PREVIOUS SURGERY IN PREGNANCY, CHILDBIRTH AND THE PUERPERIUM), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4060556' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4060556' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4060556
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4060556
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4060558 (UTERINE SCAR FROM PREVIOUS SURGERY IN PREGNANCY, CHILDBIRTH AND THE PUERPERIUM - DELIVERED), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4060558' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4060558' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4060558
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4060558
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4060559 (UTERINE SCAR FROM PREVIOUS SURGERY IN PREGNANCY, CHILDBIRTH AND THE PUERPERIUM WITH ANTENATAL PROBLEM), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4060559' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4060559' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4060559
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4060559
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4061050 (SUBACUTE AND CHRONIC VAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4061050' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4061050' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4061050
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4061050
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4071874 (PAIN IN SCROTUM), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4071874' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4071874' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4071874
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4071874
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4073700 (TRANSURETHRAL LASER PROSTATECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4073700' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4073700' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4073700
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4073700
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4081648 (ACUTE VAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4081648' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4081648' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4081648
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4081648
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4083772 (ECHOGRAPHY OF SCROTUM AND CONTENTS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4083772' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4083772' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4083772
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4083772
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4090039 (PENILE ARTERIAL INSUFFICIENCY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4090039' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4090039' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4090039
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4090039
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4092515 (MALIGNANT NEOPLASM, OVERLAPPING LESION OF CERVIX UTERI), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4092515' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4092515' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4092515
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4092515
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4093346 (LARGE PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4093346' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4093346' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4093346
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4093346
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4095940 (FINDING OF PATTERN OF MENSTRUAL CYCLE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4095940' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4095940' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4095940
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4095940
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4096783 (RADICAL PROSTATECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4096783' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4096783' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4096783
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4096783
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4109081 (PAIN IN PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4109081' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4109081' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4109081
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4109081
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4127886 (HYSTERECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4127886' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4127886' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4127886
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4127886
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4128329 (MENOPAUSE PRESENT), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4128329' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4128329' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4128329
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4128329
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4129155 (VAGINAL BLEEDING), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4129155' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4129155' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4129155
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4129155
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4138738 (VAGINAL HYSTERECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4138738' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4138738' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4138738
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4138738
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4140828 (ACUTE VULVITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4140828' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4140828' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4140828
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4140828
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4141940 (ENDOMETRIAL ABLATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4141940' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4141940' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4141940
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4141940
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4143116 (AZOOSPERMIA), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4143116' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4143116' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4143116
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4143116
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4146777 (RADICAL ABDOMINAL HYSTERECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4146777' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4146777' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4146777
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4146777
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4147021 (CONTUSION, SCROTUM OR TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4147021' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4147021' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4147021
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4147021
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4149084 (VAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4149084' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4149084' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4149084
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4149084
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4150042 (VAGINAL ULCER), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4150042' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4150042' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4150042
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4150042
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4150816 (BICORNUATE UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4150816' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4150816' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4150816
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4150816
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4155529 (MECHANICAL COMPLICATION OF INTRAUTERINE CONTRACEPTIVE DEVICE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4155529' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4155529' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4155529
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4155529
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4156113 (MALIGNANT NEOPLASM OF BODY OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4156113' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4156113' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4156113
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4156113
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4161944 (LOW CERVICAL CESAREAN SECTION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4161944' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4161944' as checkid
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
 SELECT cdmTable.*
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {{ source('omop', 'person') }} p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4161944
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4161944
) denominator
) cte
)

SELECT *
FROM cte_all
