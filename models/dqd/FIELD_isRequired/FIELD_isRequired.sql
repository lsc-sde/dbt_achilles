
select * from {{ ref( 'FIELD_isRequired_0000' ) }}
union all
select * from {{ ref( 'FIELD_isRequired_0001' ) }}
union all
select * from {{ ref( 'FIELD_isRequired_0002' ) }}
