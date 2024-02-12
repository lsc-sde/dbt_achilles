WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4162860 (PRIMARY MALIGNANT NEOPLASM OF BODY OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4162860' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4162860' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4162860
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4162860
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4163261 (MALIGNANT TUMOR OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4163261' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4163261' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4163261
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4163261
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4171394 (ABNORMAL MENSTRUAL CYCLE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4171394' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4171394' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4171394
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4171394
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4171915 (ORCHITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4171915' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4171915' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4171915
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4171915
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4180978 (VULVOVAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4180978' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4180978' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4180978
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4180978
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4181912 (CONE BIOPSY OF CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4181912' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4181912' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4181912
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4181912
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4194652 (PRURITUS OF VULVA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4194652' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4194652' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4194652
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4194652
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4199600 (CANDIDAL BALANITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4199600' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4199600' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4199600
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4199600
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4234536 (TRANSURETHRAL PROSTATECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4234536' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4234536' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4234536
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4234536
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4235215 (SWELLING OF TESTICLE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4235215' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4235215' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4235215
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4235215
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4238715 (REMOVAL OF INTRAUTERINE DEVICE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4238715' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4238715' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4238715
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4238715
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4243919 (INCISION OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4243919' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4243919' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4243919
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4243919
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4260520 (BALANITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4260520' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4260520' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4260520
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4260520
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4270932 (PAIN IN TESTICLE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4270932' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4270932' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4270932
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4270932
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4275113 (INSERTION OF INTRAUTERINE CONTRACEPTIVE DEVICE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4275113' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4275113' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4275113
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4275113
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4279913 (PRIMARY OVARIAN FAILURE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4279913' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4279913' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4279913
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4279913
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4281030 (SECONDARY MALIGNANT NEOPLASM OF RIGHT OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4281030' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4281030' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4281030
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4281030
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4294393 (ULCER OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4294393' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4294393' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4294393
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4294393
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4294805 (LAPAROSCOPIC-ASSISTED VAGINAL HYSTERECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4294805' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4294805' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4294805
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4294805
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4295261 (POSTMENOPAUSAL STATE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4295261' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4295261' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4295261
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4295261
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4303258 (BACTERIAL VAGINOSIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4303258' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4303258' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4303258
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4303258
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4306780 (GYNECOLOGIC EXAMINATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4306780' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4306780' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4306780
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4306780
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4310552 (ORCHIDOPEXY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4310552' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4310552' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4310552
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4310552
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4320332 (HYDROCELE OF TUNICA VAGINALIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4320332' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4320332' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4320332
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4320332
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4321575 (LYSIS OF PENILE ADHESIONS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4321575' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4321575' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4321575
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4321575
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4330583 (VASECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 4330583' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_4330583' as checkid
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
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 4330583
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 4330583
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 4339088 (TESTICULAR MASS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 4339088' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4339088' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4339088
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4339088
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 40481080 (BENIGN LOCALIZED HYPERPLASIA OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'40481080' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_40481080' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 40481080
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 40481080
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 40482030 (DYSPLASIA OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'40482030' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_40482030' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 40482030
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 40482030
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 40482406 (LOW LYING PLACENTA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'40482406' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_40482406' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 40482406
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 40482406
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 40483613 (INFLAMMATORY DISEASE OF FEMALE GENITAL STRUCTURE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'40483613' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_40483613' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 40483613
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 40483613
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 40490888 (HERNIATION OF RECTUM INTO VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'40490888' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_40490888' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 40490888
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 40490888
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 42709954 (PHIMOSIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'42709954' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_42709954' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 42709954
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 42709954
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 45757415 (BENIGN ENDOMETRIAL HYPERPLASIA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'45757415' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_45757415' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 45757415
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 45757415
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 45766654 (DISORDER OF SKIN OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'45766654' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_45766654' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 45766654
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 45766654
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 45770892 (PRIMARY MALIGNANT NEOPLASM OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'45770892' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_45770892' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 45770892
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 45770892
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 45772671 (NODULAR PROSTATE WITHOUT URINARY OBSTRUCTION), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,'45772671' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_45772671' as checkid
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 45772671
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 45772671
) denominator
) cte
)

SELECT *
FROM cte_all
