--HINT DISTRIBUTE_ON_KEY(analysis_id)
{{
  config(
    materialized = 'table',
    )
}}

select
  analysis_id,
  stratum_1,
  stratum_2,
  stratum_3,
  stratum_4,
  stratum_5,
  count_value
from
  (
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_0") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_3") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_4") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_5") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_10") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_11") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_12") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_101") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_102") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_108") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_109") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_110") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_111") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_112") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_113") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_116") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_117") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_119") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_200") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_201") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_202") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_204") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_207") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_209") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_210") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_212") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_220") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_221") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_225") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_226") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_300") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_301") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_303") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_325") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_400") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_401") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_402") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_404") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_405") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_414") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_415") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_416") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_420") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_425") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_500") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_501") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_502") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_504") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_505") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_525") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_600") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_601") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_602") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_604") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_605") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_620") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_625") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_630") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_691") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_700") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_701") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_702") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_704") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_705") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_720") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_725") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_791") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_800") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_801") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_802") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_804") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_805") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_807") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_814") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_820") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_822") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_823") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_825") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_826") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_827") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_891") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_900") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_901") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_902") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_904") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_920") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1000") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1001") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1002") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1004") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1020") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1100") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1101") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1102") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1103") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1200") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1201") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1202") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1203") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1300") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1301") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1302") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1304") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1312") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1320") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1321") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1325") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1326") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1408") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1409") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1410") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1411") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1412") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1413") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1425") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1800") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1801") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1802") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1804") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1805") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1807") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1811") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1814") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1818") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1819") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1820") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1821") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1822") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1823") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1825") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1826") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1827") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1891") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_1900") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2000") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2001") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2002") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2003") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2004") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2100") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2101") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2102") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2104") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2105") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2120") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2125") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2191") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2200") }}
    union all
    select
      cast(analysis_id as int) as analysis_id,
      cast(stratum_1 as varchar(255)) as stratum_1,
      cast(stratum_2 as varchar(255)) as stratum_2,
      cast(stratum_3 as varchar(255)) as stratum_3,
      cast(stratum_4 as varchar(255)) as stratum_4,
      cast(stratum_5 as varchar(255)) as stratum_5,
      cast(count_value as bigint) as count_value
    from
      {{ ref ("achilles__s_tmpach_2201") }}
  ) as q

where count_value > 5
