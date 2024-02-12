WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196163 (CERVICITIS AND ENDOCERVICITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196163' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196163' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196163
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196163
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196165 (CERVICAL INTRAEPITHELIAL NEOPLASIA GRADE 2), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196165' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196165' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196165
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196165
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196168 (IRREGULAR PERIODS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196168' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196168' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196168
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196168
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196359 (PRIMARY MALIGNANT NEOPLASM OF UTERINE CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196359' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196359' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196359
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196359
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196364 (BENIGN NEOPLASM OF UTERINE CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196364' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196364' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196364
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196364
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196473 (HYPERTROPHY OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196473' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196473' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196473
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196473
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196734 (DISORDER OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196734' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196734' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196734
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196734
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196738 (DISORDER OF MALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196738' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196738' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196738
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196738
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 196758 (TUMOR OF BODY OF UTERUS AFFECTING PREGNANCY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 196758' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_196758' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 196758
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 196758
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197032 (HYPERPLASIA OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197032' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197032' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197032
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197032
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197039 (MALE GENITAL ORGAN VASCULAR DISEASES), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197039' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197039' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197039
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197039
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197044 (FEMALE INFERTILITY ASSOCIATED WITH ANOVULATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197044' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197044' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197044
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197044
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197236 (UTERINE LEIOMYOMA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197236' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197236' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197236
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197236
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197237 (BENIGN NEOPLASM OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197237' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197237' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197237
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197237
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197507 (PRIMARY MALIGNANT NEOPLASM OF MALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197507' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197507' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197507
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197507
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197601 (SPERMATOCELE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197601' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197601' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197601
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197601
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197605 (INFLAMMATORY DISORDER OF MALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197605' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197605' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197605
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197605
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197606 (FEMALE INFERTILITY OF TUBAL ORIGIN), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197606' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197606' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197606
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197606
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197607 (EXCESSIVE AND FREQUENT MENSTRUATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197607' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197607' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197607
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197607
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197609 (CERVICAL, VAGINAL AND VULVAL INFLAMMATORY DISEASES), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197609' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197609' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197609
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197609
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197610 (CYST OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197610' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197610' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197610
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197610
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 197938 (UTERINE INERTIA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 197938' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_197938' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 197938
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 197938
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198082 (OVERLAPPING MALIGNANT NEOPLASM OF BODY OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198082' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198082' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198082
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198082
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198108 (BENIGN NEOPLASM OF FALLOPIAN TUBES AND UTERINE LIGAMENTS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198108' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198108' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198108
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198108
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198194 (FEMALE GENITAL ORGAN SYMPTOMS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198194' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198194' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198194
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198194
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198197 (MALE INFERTILITY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198197' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198197' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198197
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198197
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198198 (POLYP OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198198' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198198' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198198
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198198
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198202 (CYSTOCELE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198202' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198202' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198202
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198202
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198212 (SPOTTING PER VAGINA IN PREGNANCY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198212' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198212' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198212
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198212
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198363 (CANDIDIASIS OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198363' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198363' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198363
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198363
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198471 (COMPLEX ENDOMETRIAL HYPERPLASIA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198471' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198471' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198471
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198471
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198483 (STRICTURE OR ATRESIA OF THE VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198483' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198483' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198483
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198483
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198803 (BENIGN PROSTATIC HYPERPLASIA), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198803' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198803' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198803
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198803
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 198806 (ABSCESS OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 198806' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_198806' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 198806
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 198806
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199067 (FEMALE PELVIC INFLAMMATORY DISEASE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199067' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199067' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199067
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199067
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199078 (VAGINAL WALL PROLAPSE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199078' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199078' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199078
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199078
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199752 (SECONDARY MALIGNANT NEOPLASM OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199752' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199752' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199752
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199752
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199764 (BENIGN NEOPLASM OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199764' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199764' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199764
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199764
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199876 (PROLAPSE OF FEMALE GENITAL ORGANS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199876' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199876' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199876
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199876
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199877 (MUCOUS POLYP OF CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199877' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199877' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199877
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199877
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199878 (LIGHT AND INFREQUENT MENSTRUATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199878' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199878' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199878
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199878
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 199881 (ENDOMETRIOSIS OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 199881' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_199881' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 199881
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 199881
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200051 (PRIMARY MALIGNANT NEOPLASM OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200051' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200051' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200051
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200051
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200052 (PRIMARY MALIGNANT NEOPLASM OF UTERINE ADNEXA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200052' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200052' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200052
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200052
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200147 (ATROPHY OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200147' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200147' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200147
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200147
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200445 (CHRONIC PROSTATITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200445' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200445' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200445
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200445
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200452 (DISORDER OF FEMALE GENITAL ORGANS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200452' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200452' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200452
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200452
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200461 (ENDOMETRIOSIS OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200461' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200461' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200461
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200461
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200670 (BENIGN NEOPLASM OF MALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200670' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200670' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200670
 AND p.gender_concept_id <> 8507
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200670
) denominator
) cte UNION ALL SELECT
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200675 (NEOPLASM OF UNCERTAIN BEHAVIOR OF OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200675' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200675' as checkid
 ,0 as is_error
 ,0 as not_applicable
 ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
 ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
 ,'' as not_applicable_reason
 ,5 as threshold_value
 ,'' as notes_value
FROM (
 SELECT num_violated_rows,
 CASE
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
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200675
 AND p.gender_concept_id <> 8532
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200675
) denominator
) cte
)

SELECT *
FROM cte_all
