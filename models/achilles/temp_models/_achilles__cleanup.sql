{{
  config(
    materialized = 'view',
    description = 'Model to clean up after running achilles',
    post_hook = [
    "{{ drop_achilles_temp_models() }}",
    "drop view {{ this.include(database=false)  }}"
    ]
    )
}}

-- depends_on: {{ ref("achilles_results") }}
-- depends_on: {{ ref("achilles_results_dist")}}

select null as temp
