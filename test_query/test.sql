CREATE TABLE data_lineages_raw.test AS 
WITH table_explain AS (

	SELECT 
			((ae.object_explain::json)->>'query_id')::uuid AS query_id, 
			filters
FROM data_lineages_raw.adb_explain ae,
	jsonb_path_query(ae.object_explain::jsonb, '$.query_filters[*].filters_value[*]') AS filters 
WHERE ae.object_explain IS NOT NULL
	--AND ((ae.object_explain::json)->>'query_id')::text = 'c6feaf12-2d41-4b80-93cf-22b1864fa7cc'
	--AND ((ae.object_explain::json)->>'query_id')::text = 'fde859ab-1fa9-4cd3-a3b9-f3c5a520ea98'
),
table_with_filtes AS (
SELECT query_id,
		filters_srtuct."schema",
		filters_srtuct."table",
		filters_srtuct."field",
		filters_srtuct."operator",
		filters_srtuct."type",
		filters_srtuct."filter",
		filters_srtuct."values",
		ROW_NUMBER() OVER(PARTITION BY query_id) AS filter_number
		
FROM table_explain,
		json_to_record(table_explain.filters::json) AS filters_srtuct("schema" TEXT,  "values" TEXT[], "field" TEXT, "filter" TEXT, "table" TEXT, "operator" TEXT, "type" TEXT)
WHERE filters_srtuct."operator" <> '<>'
), table_with_unnest_filtes AS (
SELECT query_id,
		"schema",
		"table",
		"field",
		"operator",
		"type",
		"filter",
		unnest("values") AS filter_value,
		filter_number
FROM table_with_filtes
) 
SELECT query_id,
		"schema",
		"table",
		"field",
		"type",
		"operator",
		filter_value,
		CASE WHEN "type" = 'numrange' THEN 
			CASE WHEN "operator" = '=' THEN ('[' || filter_value || ','|| filter_value ||']')::numrange
				 WHEN "operator" = '<' THEN ('(,' || filter_value ||')')::numrange
				 WHEN "operator" = '>' THEN ('(' || filter_value ||',)')::numrange
				 WHEN "operator" = '<=' THEN ('(,' || filter_value ||']')::numrange
				 WHEN "operator" = '>=' THEN ('[' || filter_value ||',)')::numrange
			END
		END AS filter_numranges,
		CASE WHEN "type" = 'tsrange' THEN 
			CASE WHEN "operator" = '=' THEN ('[' || filter_value || ','|| filter_value ||']')::tsrange
				 WHEN "operator" = '<' THEN ('(,' || filter_value ||')')::tsrange
				 WHEN "operator" = '>' THEN ('(' || filter_value ||',)')::tsrange
				 WHEN "operator" = '<=' THEN ('(,' || filter_value ||']')::tsrange
				 WHEN "operator" = '>=' THEN ('[' || filter_value ||',)')::tsrange
			END
		END AS filter_tsrange,
		filter_number,
		max(filter_number) over(PARTITION BY query_id) max_filter_number
FROM table_with_unnest_filtes





WITH bound_ranges AS (
	SELECT 
		bound as lower_bound,
        lead(bound) over (order by bound) as upper_bound
    FROM (
		SELECT generate_series(-1, 15000, 500)::numeric AS bound
		) AS q
),
num_ranges AS (
	SELECT numrange(lower_bound, upper_bound, '[]') AS numrange_interval,
		   lower_bound
	FROM bound_ranges
	WHERE upper_bound IS NOT NULL
),
count_ranges AS (

SELECT 
		"lower_bound",
		t.query_id,
		count(filter_number) over(PARTITION BY "lower_bound", t.query_id) AS count_filtes,
		max(max_filter_number) over(PARTITION BY "lower_bound", t.query_id) AS max_filtes,
		n_r."numrange_interval",
		t.filter_numranges
FROM num_ranges AS n_r
LEFT JOIN data_lineages_raw.test t
	ON n_r."numrange_interval" && t.filter_numranges
		AND t."schema" = 'public'
		AND t."table" = 'somet'
		AND t.field = 'number'
)

SELECT "numrange_interval", count(DISTINCT query_id)
FROM count_ranges
WHERE max_filtes = count_filtes
	OR max_filtes IS null
GROUP BY "numrange_interval"