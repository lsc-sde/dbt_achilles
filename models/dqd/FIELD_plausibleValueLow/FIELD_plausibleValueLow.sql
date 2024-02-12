
select * from {{ ref( 'FIELD_plausibleValueLow_0000' ) }}
union all
select * from {{ ref( 'FIELD_plausibleValueLow_0001' ) }}
