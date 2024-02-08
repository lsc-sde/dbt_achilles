select
  count_value,
  count(*) as total,
  row_number() over (order by count_value) as rn
from {{ ref( "achilles__tempObs_105" ) }}
group by count_value
