-- 1410	Number of persons with continuous payer plan in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
--HINT DISTRIBUTE_ON_KEY(obs_month)
select distinct
  YEAR(payer_plan_period_start_date) * 100
  + MONTH(payer_plan_period_start_date) as obs_month,
  MAKE_DATE(
    YEAR(payer_plan_period_start_date), MONTH(payer_plan_period_start_date), 1
  ) as obs_month_start,
  LAST_DAY(payer_plan_period_start_date) as obs_month_end
from
  {{ source("omop", "payer_plan_period" ) }}
