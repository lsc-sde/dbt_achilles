-- 116	Number of persons with at least one day of observation in each year by gender and age decile
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
select distinct YEAR(observation_period_start_date) as obs_year
from
  {{ ref(  var("achilles_source_schema") + "__observation_period" ) }}
