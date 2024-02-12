
select * from {{ ref( 'FIELD_plausibleTemporalAfter_0000' ) }}
union all
select * from {{ ref( 'FIELD_plausibleTemporalAfter_0001' ) }}
