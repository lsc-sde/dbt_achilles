-- 1408	Number of persons by length of payer plan period, in 30d increments
--HINT DISTRIBUTE_ON_KEY(stratum_1)
select
  1408 as analysis_id,
  cast(
    floor(
      datediff( ppp1.payer_plan_period_end_date,ppp1.payer_plan_period_start_date
      )
      / 30
    ) as VARCHAR(255)
  ) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct p1.person_id) as count_value
from {{ source("omop", "person" ) }} as p1
inner join
  (
    select
      person_id,
      payer_plan_period_START_DATE,
      payer_plan_period_END_DATE,
      row_number() over (
        partition by person_id order by payer_plan_period_start_date asc
      ) as rn1
    from {{ source("omop", "payer_plan_period" ) }}
  ) as ppp1
  on p1.PERSON_ID = ppp1.PERSON_ID
where ppp1.rn1 = 1
group by
  cast(
    floor(
      datediff( ppp1.payer_plan_period_end_date,ppp1.payer_plan_period_start_date
      )
      / 30
    ) as VARCHAR(255)
  )
