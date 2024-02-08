-- 891	Number of total persons that have at least x observations
--HINT DISTRIBUTE_ON_KEY(stratum_1)
SELECT
  891 AS analysis_id,
  CAST(o.observation_concept_id AS VARCHAR(255)) AS stratum_1,
  CAST(o.obs_cnt AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  SUM(COUNT(o.person_id))
    OVER (PARTITION BY o.observation_concept_id ORDER BY o.obs_cnt DESC)
    AS count_value
FROM (
  SELECT
    o.observation_concept_id,
    o.person_id,
    COUNT(o.observation_id) AS obs_cnt
  FROM
    {{ source("omop", "observation" ) }} AS o
  INNER JOIN
    {{ source("omop", "observation_period" ) }} AS op
    ON
      o.person_id = op.person_id
      AND
      o.observation_date >= op.observation_period_start_date
      AND
      o.observation_date <= op.observation_period_end_date
  GROUP BY
    o.person_id,
    o.observation_concept_id
) AS o
GROUP BY
  o.observation_concept_id,
  o.obs_cnt
