-- 791	Number of total persons that have at least x drug exposures
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  791 AS analysis_id,
  CAST(de.drug_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(de.drg_cnt AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  SUM(COUNT(de.person_id))
    OVER (PARTITION BY de.drug_concept_id ORDER BY de.drg_cnt DESC)
    AS count_value
FROM (
  SELECT
    de.drug_concept_id,
    de.person_id,
    COUNT(de.drug_exposure_id) AS drg_cnt
  FROM
    {{ ref(  var("achilles_source_schema") + "__drug_exposure" ) }} AS de
  INNER JOIN
    {{ ref(  var("achilles_source_schema") + "__observation_period" ) }} AS op
    ON
      de.person_id = op.person_id
      AND
      de.drug_exposure_start_date >= op.observation_period_start_date
      AND
      de.drug_exposure_start_date <= op.observation_period_end_date
  GROUP BY
    de.person_id,
    de.drug_concept_id
) AS de
GROUP BY
  de.drug_concept_id,
  de.drg_cnt
