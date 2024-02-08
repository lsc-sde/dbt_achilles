--HINT DISTRIBUTE_ON_KEY(stratum1_id)
select
  1817 as analysis_id,
  cast(o.stratum1_id as VARCHAR(255)) as stratum1_id,
  cast(o.stratum2_id as VARCHAR(255)) as stratum2_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(
    case
      when p.accumulated >= .50 * o.total then count_value else o.max_value
    end
  ) as median_value,
  min(
    case
      when p.accumulated >= .10 * o.total then count_value else o.max_value
    end
  ) as p10_value,
  min(
    case
      when p.accumulated >= .25 * o.total then count_value else o.max_value
    end
  ) as p25_value,
  min(
    case
      when p.accumulated >= .75 * o.total then count_value else o.max_value
    end
  ) as p75_value,
  min(
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
      sum(p.total) as accumulated
    from {{ ref( "achilles__statsView_1817" ) }} as s
    inner join
      {{ ref( "achilles__statsView_1817" ) }} as p
      on
        s.stratum1_id = p.stratum1_id
        and s.stratum2_id = p.stratum2_id
        and s.rn >= p.rn
    group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
  ) as p
inner join
  {{ ref('achilles__overallStats_1817') }} as o
  on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
group by
  o.stratum1_id,
  o.stratum2_id,
  o.total,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value
