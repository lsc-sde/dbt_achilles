WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 26662 (TESTICULAR HYPOFUNCTION), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 26662' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_26662' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 26662
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 26662
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 26935 (DISORDER OF ENDOCRINE TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 26935' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_26935' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 26935
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 26935
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 30969 (TESTICULAR HYPERFUNCTION), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 30969' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_30969' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 30969
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 30969
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 73801 (SCROTAL VARICES), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 73801' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_73801' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 73801
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 73801
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 74322 (BENIGN NEOPLASM OF SCROTUM), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 74322' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_74322' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 74322
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 74322
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 78193 (ORCHITIS AND EPIDIDYMITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 78193' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_78193' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 78193
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 78193
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 79758 (PRIMARY MALIGNANT NEOPLASM OF SCROTUM), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 79758' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_79758' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 79758
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 79758
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 141917 (BALANITIS XEROTICA OBLITERANS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 141917' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_141917' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 141917
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 141917
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 192367 (DYSPLASIA OF CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 192367' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_192367' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 192367
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 192367
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 192676 (CERVICAL INTRAEPITHELIAL NEOPLASIA GRADE 1), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 192676' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_192676' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 192676
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 192676
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 192683 (UTEROVAGINAL PROLAPSE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 192683' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_192683' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 192683
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 192683
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 192854 (INTRAMURAL LEIOMYOMA OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 192854' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_192854' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 192854
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 192854
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 192858 (OVARIAN HYPERFUNCTION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 192858' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_192858' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 192858
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 192858
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193254 (DISORDER OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193254' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193254' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193254
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193254
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193261 (VAGINOSPASM), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193261' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193261' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193261
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193261
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193262 (INFLAMMATORY DISORDER OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193262' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193262' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193262
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193262
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193277 (DELIVERIES BY CESAREAN), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193277' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193277' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193277
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193277
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193437 (NEOPLASM OF UNCERTAIN BEHAVIOR OF FEMALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193437' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193437' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193437
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193437
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193439 (BENIGN NEOPLASM OF BODY OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193439' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193439' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193439
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193439
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193522 (ACUTE PROSTATITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193522' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193522' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193522
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193522
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193530 (FOLLICULAR CYST OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193530' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193530' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193530
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193530
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193739 (OVARIAN FAILURE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193739' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193739' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193739
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193739
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 193818 (CALCULUS OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 193818' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_193818' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 193818
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 193818
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194092 (UTERINE PROLAPSE WITHOUT VAGINAL WALL PROLAPSE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194092' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194092' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194092
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194092
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194286 (MALIGNANT NEOPLASM OF CORPUS UTERI, EXCLUDING ISTHMUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194286' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194286' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194286
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194286
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194412 (DYSPLASIA OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194412' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194412' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194412
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194412
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194420 (ENDOMETRIOSIS OF FALLOPIAN TUBE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194420' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194420' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194420
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194420
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194611 (CARCINOMA IN SITU OF UTERINE CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194611' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194611' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194611
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194611
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194696 (DYSMENORRHEA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194696' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194696' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194696
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194696
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194871 (TRICHOMONAL VULVOVAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194871' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194871' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194871
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194871
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 194997 (PROSTATITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 194997' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_194997' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 194997
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 194997
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195009 (LEUKOPLAKIA OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195009' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195009' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195009
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195009
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195012 (INTERMENSTRUAL BLEEDING - IRREGULAR), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195012' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195012' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195012
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195012
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195197 (PRIMARY MALIGNANT NEOPLASM OF VULVA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195197' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195197' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195197
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195197
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195316 (ATYPICAL ENDOMETRIAL HYPERPLASIA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195316' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195316' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195316
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195316
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195321 (POSTMENOPAUSAL BLEEDING), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195321' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195321' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195321
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195321
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195483 (PRIMARY MALIGNANT NEOPLASM OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195483' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195483' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195483
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195483
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195500 (BENIGN NEOPLASM OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195500' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195500' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195500
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195500
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195501 (POLYCYSTIC OVARIES), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195501' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195501' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195501
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195501
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195683 (OPEN WOUND OF PENIS WITHOUT COMPLICATION), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195683' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195683' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195683
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195683
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195769 (SUBMUCOUS LEIOMYOMA OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195769' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195769' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195769
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195769
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195770 (SUBSEROUS LEIOMYOMA OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195770' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195770' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195770
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195770
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195793 (NEOPLASM OF UNCERTAIN BEHAVIOR OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195793' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195793' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195793
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195793
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195867 (NONINFLAMMATORY DISORDER OF THE VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195867' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195867' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195867
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195867
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 195873 (LEUKORRHEA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 195873' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_195873' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 195873
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 195873
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196048 (PRIMARY MALIGNANT NEOPLASM OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196048' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196048' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196048
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196048
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196051 (OVERLAPPING MALIGNANT NEOPLASM OF FEMALE GENITAL ORGANS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196051' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196051' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196051
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196051
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196068 (CARCINOMA IN SITU OF MALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196068' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196068' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196068
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196068
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196157 (INDURATIO PENIS PLASTICA), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196157' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196157' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196157
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196157
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196158 (DISORDER OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196158' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196158' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196158
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196158
) denominator
) cte
)

SELECT *
FROM cte_all
