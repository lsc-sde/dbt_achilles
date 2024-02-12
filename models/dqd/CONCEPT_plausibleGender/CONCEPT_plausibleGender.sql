
select * from {{ ref( 'CONCEPT_plausibleGender_0000' ) }}
union all
select * from {{ ref( 'CONCEPT_plausibleGender_0001' ) }}
union all
select * from {{ ref( 'CONCEPT_plausibleGender_0002' ) }}
union all
select * from {{ ref( 'CONCEPT_plausibleGender_0003' ) }}
union all
select * from {{ ref( 'CONCEPT_plausibleGender_0004' ) }}
union all
select * from {{ ref( 'CONCEPT_plausibleGender_0005' ) }}
