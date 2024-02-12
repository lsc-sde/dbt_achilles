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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200775 (ENDOMETRIAL HYPERPLASIA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200775' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200775' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200775
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200775
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200779 (POLYP OF CORPUS UTERI), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200779' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200779' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200779
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200779
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200780 (DISORDER OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200780' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200780' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200780
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200780
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200962 (PRIMARY MALIGNANT NEOPLASM OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200962' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200962' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200962
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200962
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 200970 (CARCINOMA IN SITU OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 200970' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_200970' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 200970
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 200970
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201072 (BENIGN PROSTATIC HYPERTROPHY WITHOUT OUTFLOW OBSTRUCTION), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201072' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201072' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201072
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201072
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201078 (ATROPHIC VAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201078' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201078' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201078
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201078
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201211 (HERPETIC VULVOVAGINITIS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201211' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201211' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201211
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201211
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201238 (PRIMARY MALIGNANT NEOPLASM OF FEMALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201238' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201238' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201238
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201238
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201244 (BENIGN NEOPLASM OF VAGINA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201244' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201244' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201244
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201244
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201257 (DISORDER OF ENDOCRINE OVARY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201257' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201257' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201257
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201257
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201346 (EDEMA OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201346' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201346' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201346
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201346
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201355 (EROSION AND ECTROPION OF THE CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201355' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201355' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201355
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201355
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201527 (NEOPLASM OF UNCERTAIN BEHAVIOR OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201527' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201527' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201527
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201527
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201617 (PROSTATIC CYST), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201617' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201617' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201617
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201617
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201625 (MALPOSITION OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201625' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201625' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201625
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201625
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201801 (PRIMARY MALIGNANT NEOPLASM OF FALLOPIAN TUBE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201801' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201801' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201801
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201801
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201817 (BENIGN NEOPLASM OF FEMALE GENITAL ORGAN), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201817' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201817' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201817
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201817
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201823 (BENIGN NEOPLASM OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201823' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201823' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201823
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201823
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201907 (EDEMA OF MALE GENITAL ORGANS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201907' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201907' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201907
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201907
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201909 (FEMALE INFERTILITY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201909' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201909' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201909
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201909
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 201913 (TORSION OF THE OVARY, OVARIAN PEDICLE OR FALLOPIAN TUBE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 201913' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_201913' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 201913
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 201913
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 314409 (VASCULAR DISORDER OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 314409' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_314409' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 314409
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 314409
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 315586 (PRIAPISM), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 315586' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_315586' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 315586
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 315586
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 433716 (PRIMARY MALIGNANT NEOPLASM OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 433716' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_433716' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 433716
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 433716
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 434251 (INJURY OF MALE EXTERNAL GENITAL ORGANS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 434251' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_434251' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 434251
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 434251
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 435315 (TORSION OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 435315' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_435315' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 435315
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 435315
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 435648 (RETRACTILE TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 435648' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_435648' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 435648
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 435648
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 436155 (REDUNDANT PREPUCE AND PHIMOSIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 436155' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_436155' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 436155
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 436155
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 436358 (PRIMARY MALIGNANT NEOPLASM OF EXOCERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 436358' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_436358' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 436358
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 436358
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 436366 (BENIGN NEOPLASM OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 436366' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_436366' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 436366
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 436366
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 436466 (BALANOPOSTHITIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 436466' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_436466' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 436466
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 436466
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 437501 (PRIMARY MALIGNANT NEOPLASM OF LABIA MAJORA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 437501' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_437501' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 437501
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 437501
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 437655 (UNDESCENDED TESTICLE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 437655' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_437655' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 437655
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 437655
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 438477 (ATROPHY OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 438477' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_438477' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 438477
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 438477
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 439871 (HEMOSPERMIA), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 439871' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_439871' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 439871
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 439871
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 440971 (NEOPLASM OF UNCERTAIN BEHAVIOR OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 440971' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_440971' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 440971
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 440971
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 441068 (TORSION OF APPENDIX OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 441068' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_441068' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 441068
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 441068
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 441077 (STENOSIS OF CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 441077' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_441077' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 441077
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 441077
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 441805 (PRIMARY MALIGNANT NEOPLASM OF ENDOCERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 441805' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_441805' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 441805
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 441805
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 442781 (DISORDER OF UTERINE CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 442781' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_442781' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 442781
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 442781
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 443211 (BENIGN PROSTATIC HYPERTROPHY WITH OUTFLOW OBSTRUCTION), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 443211' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_443211' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 443211
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 443211
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 443435 (PRIMARY UTERINE INERTIA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 443435' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_443435' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 443435
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 443435
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 443800 (AMENORRHEA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 443800' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_443800' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 443800
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 443800
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 444078 (INFLAMMATION OF CERVIX), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 444078' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_444078' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 444078
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 444078
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 444106 (CANDIDIASIS OF VULVA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'CONDITION_OCCURRENCE' as cdm_table_name
 ,'CONDITION_CONCEPT_ID' as cdm_field_name
 ,' 444106' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_condition_occurrence_condition_concept_id_444106' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 444106
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 444106
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2003947 (CLOSED [PERCUTANEOUS] [NEEDLE] BIOPSY OF PROSTATE), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2003947' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2003947' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2003947
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2003947
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2003966 (OTHER TRANSURETHRAL PROSTATECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2003966' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2003966' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2003966
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2003966
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2003983 (OTHER PROSTATECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2003983' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2003983' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2003983
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2003983
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004031 (OTHER REPAIR OF SCROTUM AND TUNICA VAGINALIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004031' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004031' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004031
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004031
) denominator
) cte
)
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
WITH cte_all  AS (SELECT cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 , CAST('' as STRING) as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004063 (UNILATERAL ORCHIECTOMY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004063' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004063' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004063
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004063
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004070 (OTHER REPAIR OF TESTIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004070' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004070' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004070
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004070
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004090 (EXCISION OF VARICOCELE AND HYDROCELE OF SPERMATIC CORD), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004090' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004090' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004090
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004090
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004164 (LOCAL EXCISION OR DESTRUCTION OF LESION OF PENIS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004164' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004164' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004164
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004164
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004263 (OTHER REMOVAL OF BOTH OVARIES AND TUBES AT SAME OPERATIVE EPISODE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004263' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004263' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004263
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004263
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004329 (OTHER BILATERAL DESTRUCTION OR OCCLUSION OF FALLOPIAN TUBES), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004329' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004329' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004329
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004329
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004342 (REMOVAL OF BOTH FALLOPIAN TUBES AT SAME OPERATIVE EPISODE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004342' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004342' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004342
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004342
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004443 (CLOSED BIOPSY OF UTERUS), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004443' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004443' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004443
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004443
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2004627 (VAGINAL SUSPENSION AND FIXATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2004627' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004627' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004627
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004627
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109825 (TRANSURETHRAL ELECTROSURGICAL RESECTION OF PROSTATE, INCLUDING CONTROL OF POSTOPERATIVE BLEEDING, COMPLETE (VASECTOMY, MEATOTOMY, CYSTOURETHROSCOPY, URETHRAL CALIBRATION AND/OR DILATION, AND INTERNAL URETHROTOMY ARE INCLUDED)), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109825' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109825' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109825
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109825
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109833 (LASER VAPORIZATION OF PROSTATE, INCLUDING CONTROL OF POSTOPERATIVE BLEEDING, COMPLETE (VASECTOMY, MEATOTOMY, CYSTOURETHROSCOPY, URETHRAL CALIBRATION AND/OR DILATION, INTERNAL URETHROTOMY AND TRANSURETHRAL RESECTION OF PROSTATE ARE INCLUDED IF PERFORMED)), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109833' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109833' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109833
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109833
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109900 (DESTRUCTION OF LESION(S), PENIS (EG, CONDYLOMA, PAPILLOMA, MOLLUSCUM CONTAGIOSUM, HERPETIC VESICLE), SIMPLE CHEMICAL), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109900' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109900' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109900
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109900
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109902 (DESTRUCTION OF LESION(S), PENIS (EG, CONDYLOMA, PAPILLOMA, MOLLUSCUM CONTAGIOSUM, HERPETIC VESICLE), SIMPLE CRYOSURGERY), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109902' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109902' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109902
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109902
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109905 (DESTRUCTION OF LESION(S), PENIS (EG, CONDYLOMA, PAPILLOMA, MOLLUSCUM CONTAGIOSUM, HERPETIC VESICLE), EXTENSIVE (EG, LASER SURGERY, ELECTROSURGERY, CRYOSURGERY, CHEMOSURGERY)), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109905' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109905' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109905
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109905
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109906 (BIOPSY OF PENIS, (SEPARATE PROCEDURE)), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109906' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109906' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109906
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109906
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109916 (CIRCUMCISION, USING CLAMP OR OTHER DEVICE WITH REGIONAL DORSAL PENILE OR RING BLOCK), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109916' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109916' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109916
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109916
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109968 (FORESKIN MANIPULATION INCLUDING LYSIS OF PREPUTIAL ADHESIONS AND STRETCHING), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109968' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109968' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109968
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109968
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109973 (ORCHIECTOMY, SIMPLE (INCLUDING SUBCAPSULAR), WITH OR WITHOUT TESTICULAR PROSTHESIS, SCROTAL OR INGUINAL APPROACH), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109973' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109973' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109973
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109973
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2109981 (ORCHIOPEXY, INGUINAL APPROACH, WITH OR WITHOUT HERNIA REPAIR), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2109981' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2109981' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2109981
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2109981
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110004 (DRAINAGE OF SCROTAL WALL ABSCESS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110004' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110004' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110004
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110004
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110011 (VASECTOMY, UNILATERAL OR BILATERAL (SEPARATE PROCEDURE), INCLUDING POSTOPERATIVE SEMEN EXAMINATION(S)), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110011' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110011' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110011
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110011
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110026 (BIOPSY, PROSTATE NEEDLE OR PUNCH, SINGLE OR MULTIPLE, ANY APPROACH), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110026' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110026' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110026
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110026
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110039 (PROSTATECTOMY, RETROPUBIC RADICAL, WITH OR WITHOUT NERVE SPARING WITH BILATERAL PELVIC LYMPHADENECTOMY, INCLUDING EXTERNAL ILIAC, HYPOGASTRIC, AND OBTURATOR NODES), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110039' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110039' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110039
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110039
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110044 (LAPAROSCOPY, SURGICAL PROSTATECTOMY, RETROPUBIC RADICAL, INCLUDING NERVE SPARING, INCLUDES ROBOTIC ASSISTANCE, WHEN PERFORMED), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110044' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110044' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110044
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110044
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110078 (COLPOSCOPY OF THE VULVA), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110078' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110078' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110078
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110078
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110116 (COLPOPEXY, VAGINAL EXTRA-PERITONEAL APPROACH (SACROSPINOUS, ILIOCOCCYGEUS)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110116' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110116' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110116
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110116
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110142 (LAPAROSCOPY, SURGICAL, COLPOPEXY (SUSPENSION OF VAGINAL APEX)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110142' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110142' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110142
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110142
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110144 (COLPOSCOPY OF THE CERVIX INCLUDING UPPER/ADJACENT VAGINA, WITH BIOPSY(S) OF THE CERVIX AND ENDOCERVICAL CURETTAGE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110144' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110144' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110144
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110144
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110169 (ENDOMETRIAL SAMPLING (BIOPSY) WITH OR WITHOUT ENDOCERVICAL SAMPLING (BIOPSY), WITHOUT CERVICAL DILATION, ANY METHOD (SEPARATE PROCEDURE)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110169' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110169' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110169
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110169
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110175 (TOTAL ABDOMINAL HYSTERECTOMY (CORPUS AND CERVIX), WITH OR WITHOUT REMOVAL OF TUBE(S), WITH OR WITHOUT REMOVAL OF OVARY(S)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110175' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110175' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110175
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110175
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110194 (INSERTION OF INTRAUTERINE DEVICE (IUD)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110194' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110194' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110194
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110194
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110195 (REMOVAL OF INTRAUTERINE DEVICE (IUD)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110195' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110195' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110195
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110195
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110203 (ENDOMETRIAL ABLATION, THERMAL, WITHOUT HYSTEROSCOPIC GUIDANCE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110203' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110203' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110203
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110203
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110222 (HYSTEROSCOPY, SURGICAL WITH SAMPLING (BIOPSY) OF ENDOMETRIUM AND/OR POLYPECTOMY, WITH OR WITHOUT D & C), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110222' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110222' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110222
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110222
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110227 (HYSTEROSCOPY, SURGICAL WITH ENDOMETRIAL ABLATION (EG, ENDOMETRIAL RESECTION, ELECTROSURGICAL ABLATION, THERMOABLATION)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110227' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110227' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110227
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110227
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110230 (LAPAROSCOPY, SURGICAL, WITH TOTAL HYSTERECTOMY, FOR UTERUS 250 G OR LESS WITH REMOVAL OF TUBE(S) AND/OR OVARY(S)), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110230' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110230' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110230
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110230
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110307 (ROUTINE OBSTETRIC CARE INCLUDING ANTEPARTUM CARE, VAGINAL DELIVERY (WITH OR WITHOUT EPISIOTOMY, AND/OR FORCEPS) AND POSTPARTUM CARE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110307' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110307' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110307
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110307
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110315 (ROUTINE OBSTETRIC CARE INCLUDING ANTEPARTUM CARE, CESAREAN DELIVERY, AND POSTPARTUM CARE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110315' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110315' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110315
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110315
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110316 (CESAREAN DELIVERY ONLY), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110316' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110316' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110316
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110316
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110317 (CESAREAN DELIVERY ONLY, INCLUDING POSTPARTUM CARE), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110317' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110317' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110317
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110317
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2110326 (TREATMENT OF MISSED ABORTION, COMPLETED SURGICALLY FIRST TRIMESTER), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2110326' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2110326' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2110326
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2110326
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211747 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, FETAL AND MATERNAL EVALUATION, FIRST TRIMESTER (< 14 WEEKS 0 DAYS), TRANSABDOMINAL APPROACH SINGLE OR FIRST GESTATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211747' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211747' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211747
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211747
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211749 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, FETAL AND MATERNAL EVALUATION, AFTER FIRST TRIMESTER (> OR = 14 WEEKS 0 DAYS), TRANSABDOMINAL APPROACH SINGLE OR FIRST GESTATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211749' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211749' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211749
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211749
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211751 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, FETAL AND MATERNAL EVALUATION PLUS DETAILED FETAL ANATOMIC EXAMINATION, TRANSABDOMINAL APPROACH SINGLE OR FIRST GESTATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211751' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211751' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211751
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211751
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211753 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, FIRST TRIMESTER FETAL NUCHAL TRANSLUCENCY MEASUREMENT, TRANSABDOMINAL OR TRANSVAGINAL APPROACH SINGLE OR FIRST GESTATION), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211753' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211753' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211753
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211753
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211755 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, LIMITED (EG, FETAL HEART BEAT, PLACENTAL LOCATION, FETAL POSITION AND/OR QUALITATIVE AMNIOTIC FLUID VOLUME), 1 OR MORE FETUSES), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211755' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211755' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211755
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211755
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211756 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, FOLLOW-UP (EG, RE-EVALUATION OF FETAL SIZE BY MEASURING STANDARD GROWTH PARAMETERS AND AMNIOTIC FLUID VOLUME, RE-EVALUATION OF ORGAN SYSTEM(S) SUSPECTED OR CONFIRMED TO BE ABNORMAL ON A PREV), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211756' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211756' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211756
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211756
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211757 (ULTRASOUND, PREGNANT UTERUS, REAL TIME WITH IMAGE DOCUMENTATION, TRANSVAGINAL), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211757' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211757' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211757
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211757
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211765 (ULTRASOUND, TRANSVAGINAL), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211765' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211765' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211765
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211765
) denominator
) cte UNION ALL SELECT 
 cte.num_violated_rows
 ,cte.pct_violated_rows
 ,cte.num_denominator_rows
 ,'' as execution_time
 ,'' as query_text
 ,'plausibleGender' as check_name
 ,'CONCEPT' as check_level
 ,'For a CONCEPT_ID 2211769 (ULTRASOUND, SCROTUM AND CONTENTS), the number and percent of records associated with patients with an implausible gender (correct gender = MALE).' as check_description
 ,'PROCEDURE_OCCURRENCE' as cdm_table_name
 ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
 ,' 2211769' as concept_id
 ,'NA' as unit_concept_id
 ,'concept_plausible_gender.sql' as sql_file
 ,'Plausibility' as category
 ,'Atemporal' as subcategory
 ,'Validation' as context
 ,'' as warning
 ,'' as error
 ,'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2211769' as checkid
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2211769
 AND p.gender_concept_id <> 8507 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2211769
) denominator
) cte
)
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
 SELECT cdmTable.* 
 FROM hive_metastore.dev_vc.CONDITION_OCCURRENCE cdmTable
 INNER JOIN hive_metastore.dev_vc.person p
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
INSERT INTO hive_metastore.dev_vc_achilles.dqdashboard_results
SELECT *
FROM cte_all;
