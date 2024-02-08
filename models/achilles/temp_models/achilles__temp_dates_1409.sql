-- 1409	Number of persons with continuous payer plan in each year
-- Note: using temp table instead of nested query because this gives vastly improved
select distinct YEAR(payer_plan_period_start_date) as obs_year
from
  {{ ref(  var("achilles_source_schema") + "__payer_plan_period" ) }}
