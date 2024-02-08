with rawData as (
  select
    2004 as analysis_id,
    cast('0000001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ ref( "achilles__obs" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0000010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ ref( "achilles__prococ" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0000011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0000100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ source("omop", "death" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0000101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select count(
        *) as count_value from (
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0000110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select count(
        *) as count_value from (
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0000111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select count(
        *) as count_value from (
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ ref( "achilles__msmt" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0001111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ ref( "achilles__dvexp" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0010111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0011111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ ref( "achilles__drexp" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0100111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0101111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0110111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('0111111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (select count(*) as count_value from (select person_id from {{ ref( "achilles__conoc" ) }}) as subquery) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1000111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1001111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1010111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1011111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1100111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1101111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1110111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111000' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111001' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111010' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111011' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111100' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111101' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111110' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
  union all
  select
    2004 as analysis_id,
    cast('1111111' as VARCHAR(255)) as stratum_1,
    cast(
      (
        1.0 * personIntersection.count_value / totalPersonsDb.totalPersons
      ) as VARCHAR(255)
    ) as stratum_2,
    cast(NULL as VARCHAR(255)) as stratum_3,
    cast(NULL as VARCHAR(255)) as stratum_4,
    cast(NULL as VARCHAR(255)) as stratum_5,
    personIntersection.count_value
  from
    (
      select
        count(
          *
        ) as count_value
      from (
        select person_id from {{ ref( "achilles__conoc" ) }}
        intersect
        select person_id from {{ ref( "achilles__drexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__dvexp" ) }}
        intersect
        select person_id from {{ ref( "achilles__msmt" ) }}
        intersect
        select person_id from {{ source("omop", "death" ) }}
        intersect
        select person_id from {{ ref( "achilles__prococ" ) }}
        intersect
        select person_id from {{ ref( "achilles__obs" ) }}
      ) as subquery
    ) as personIntersection,
    (
      select count(distinct person_id) as totalPersons
      from {{ source("omop", "person" ) }}
    ) as totalPersonsDb
)

select * from rawData
