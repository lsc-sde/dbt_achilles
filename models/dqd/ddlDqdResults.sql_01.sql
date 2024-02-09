

CREATE TABLE hive_metastore.dev_vc_achilles.dqdashboard_results
USING DELTA
 AS
SELECT
CAST(NULL AS bigint) AS num_violated_rows,

	CAST(NULL AS float) AS pct_violated_rows,

	CAST(NULL AS bigint) AS num_denominator_rows,

	CAST(NULL AS STRING) AS execution_time,

	CAST(NULL AS STRING) AS query_text,

	CAST(NULL AS STRING) AS check_name,

	CAST(NULL AS STRING) AS check_level,

	CAST(NULL AS STRING) AS check_description,

	CAST(NULL AS STRING) AS cdm_table_name,

	CAST(NULL AS STRING) AS cdm_field_name,

	CAST(NULL AS STRING) AS concept_id,

	CAST(NULL AS STRING) AS unit_concept_id,

	CAST(NULL AS STRING) AS sql_file,

	CAST(NULL AS STRING) AS category,

	CAST(NULL AS STRING) AS subcategory,

	CAST(NULL AS STRING) AS context,

	CAST(NULL AS STRING) AS warning,

	CAST(NULL AS STRING) AS error,

	CAST(NULL AS STRING) AS checkid,

	CAST(NULL AS integer) AS is_error,

	CAST(NULL AS integer) AS not_applicable,

	CAST(NULL AS integer) AS failed,

	CAST(NULL AS integer) AS passed,

	CAST(NULL AS STRING) AS not_applicable_reason,

	CAST(NULL AS integer) AS threshold_value,

	CAST(NULL AS STRING) AS notes_value  WHERE 1 = 0