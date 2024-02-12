
select * from {{ ref( 'FIELD_plausibleValueHigh_0000' ) }}
union all
select * from {{ ref( 'FIELD_plausibleValueHigh_0001' ) }}
