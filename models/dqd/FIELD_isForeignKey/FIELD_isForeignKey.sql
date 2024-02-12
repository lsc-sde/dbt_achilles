
select * from {{ ref( 'FIELD_isForeignKey_0000' ) }}
union all
select * from {{ ref( 'FIELD_isForeignKey_0001' ) }}
union all
select * from {{ ref( 'FIELD_isForeignKey_0002' ) }}
union all
select * from {{ ref( 'FIELD_isForeignKey_0003' ) }}
