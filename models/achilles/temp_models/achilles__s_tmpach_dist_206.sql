--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  analysis_id,
  stratum1_id as stratum_1,
  stratum2_id as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count_value,
  min_value,
  max_value,
  avg_value,
  stdev_value,
  median_value,
  p10_value,
  p25_value,
  p75_value,
  p90_value
from {{ ref( "achilles__tempResults_206" ) }}
