--HINT DISTRIBUTE_ON_KEY(stratum1_id)
select
  1815 as analysis_id,
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
    from {{ ref( "achilles__statsView_1815" ) }} as s
    inner join
      {{ ref( "achilles__statsView_1815" ) }} as p
      on
        s.stratum1_id = p.stratum1_id
        and s.stratum2_id = p.stratum2_id
        and s.rn >= p.rn
    group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
  ) as p
inner join
  (
    select
      m.subject_id as stratum1_id,
      m.unit_concept_id as stratum2_id,
      cast(avg(1.0 * m.count_value) as FLOAT) as avg_value,
      cast(stddev(m.count_value) as FLOAT) as stdev_value,
      min(m.count_value) as min_value,
      max(m.count_value) as max_value,
      count(*) as total
    from
      (
        select
          m.measurement_concept_id as subject_id,
          m.unit_concept_id,
          cast(m.value_as_number as FLOAT) as count_value
        from
          {{ source("omop", "measurement" ) }} as m
        inner join
          {{ source("omop", "observation_period" ) }} as op
          on
            m.person_id = op.person_id
            and
            m.measurement_date >= op.observation_period_start_date
            and
            m.measurement_date <= op.observation_period_end_date
        where
          m.unit_concept_id is not null
          and
          m.value_as_number is not null
      ) as m
    group by
      m.subject_id,
      m.unit_concept_id
  ) as o
  on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
group by
  o.stratum1_id,
  o.stratum2_id,
  o.total,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value
