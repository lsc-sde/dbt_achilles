
select * from {{ ref( 'FIELD_measureValueCompleteness_0000' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0001' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0002' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0003' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0004' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0005' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0006' ) }}
union all
select * from {{ ref( 'FIELD_measureValueCompleteness_0007' ) }}
