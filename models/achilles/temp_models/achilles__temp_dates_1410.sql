-- 1410	Number of persons with continuous payer plan in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(obs_month)
SELECT DISTINCT
  YEAR(payer_plan_period_start_date) * 100
  + MONTH(payer_plan_period_start_date) AS obs_month,
  DATEFROMPARTS(
    YEAR(payer_plan_period_start_date), MONTH(payer_plan_period_start_date), 1
  ) AS obs_month_start,
  EOMONTH(payer_plan_period_start_date) AS obs_month_end
FROM
  {{ source("omop", "payer_plan_period" ) }}
