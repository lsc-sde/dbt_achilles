--HINT DISTRIBUTE_ON_KEY(stratum1_id)
select
  1816 as analysis_id,
  cast(o.stratum1_id as VARCHAR(255)) as stratum1_id,
  cast(o.stratum2_id as VARCHAR(255)) as stratum2_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  MIN(
    case
      when p.accumulated >= .50 * o.total then count_value else o.max_value
    end
  ) as median_value,
  MIN(
    case
      when p.accumulated >= .10 * o.total then count_value else o.max_value
    end
  ) as p10_value,
  MIN(
    case
      when p.accumulated >= .25 * o.total then count_value else o.max_value
    end
  ) as p25_value,
  MIN(
    case
      when p.accumulated >= .75 * o.total then count_value else o.max_value
    end
  ) as p75_value,
  MIN(
    case
      when p.accumulated >= .90 * o.total then count_value else o.max_value
    end
  ) as p90_value
from
  (
    select
      s.stratum1_id,
      s.stratum2_id,
      s.count_value,
      s.total,
      SUM(p.total) as accumulated
    from {{ ref( "achilles__statsView_1816" ) }} as s
    inner join
      achilles__statsView_1816 as p
      on
        s.stratum1_id = p.stratum1_id
        and s.stratum2_id = p.stratum2_id
        and p.rn <= s.rn
    group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
  ) as p
inner join
  achilles__overallStats_1816 as o
  on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
group by
  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
