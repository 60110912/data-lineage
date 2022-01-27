CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA data_lineages_raw;

CREATE TABLE data_lineages_raw.db_explain (
    pk_db_explain bigserial NOT NULL PRIMARY KEY,
    pg_time timestamp,
    timezone text,
    duration_time text,
    query_plain text,
    object_explain text,
    created_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    updated_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    is_porcessed bool
);

CREATE OR REPLACE function fn_sql_to_hash(
        p_sql_query TEXT
    )
returns table (
        query_hash TEXT
    )
language plpgsql
as
$$
begin
    return Query 
        SELECT md5(array_to_string(regexp_split_to_array(lower('hello world    ' || chr(9)||'sdsds'), '\s+'),' '));
end
$$;

CREATE TABLE data_lineages_marts.f_query_with_filters (
	pk_query_with_filters bigserial NOT NULL,
	fk_db_explain int8 NULL,
	query_id uuid NOT NULL,
	"schema" text NULL,
	"table" text NULL,
	field text NULL,
	"type" text NULL,
	"operator" text NULL,
	filter_value text NULL,
	filter_numranges numrange NULL,
	filter_tsrange tsrange NULL,
	filter_number int8 NULL,
	max_filter_number int8 NULL,
	created_dttm timestamp NULL DEFAULT 'now'::text::timestamp without time zone,
	pg_time timestamp NULL,
	timezone text NULL,
	CONSTRAINT f_query_with_filters_pkey PRIMARY KEY (pk_query_with_filters)
);

CREATE OR REPLACE FUNCTION data_lineages_marts.fn_load_f_query_with_filters() RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN

	UPDATE data_lineages_raw.db_explain
	SET is_porcessed = FALSE 
	WHERE is_porcessed IS NULL;

    drop table if exists tt_explain_object;
    create temp table tt_explain_object as
    WITH table_explain AS (
		SELECT 
			pk_db_explain AS fk_db_explain,
			((d_e.object_explain::json)->>'query_id')::uuid AS query_id, 
			filters,
			pg_time, 
			timezone
	    FROM 
	    	data_lineages_raw.db_explain d_e,
			jsonb_path_query(d_e.object_explain::jsonb, '$.query_filters[*].filters_value[*]') AS filters 
		WHERE 
			d_e.object_explain IS NOT NULL
		  	AND d_e.is_porcessed = false
	),
	table_with_filtes AS (
		SELECT 
				fk_db_explain,
				query_id,
				filters_srtuct."schema",
				filters_srtuct."table",
				filters_srtuct."field",
				filters_srtuct."operator",
				filters_srtuct."type",
				filters_srtuct."filter",
				filters_srtuct."values",
				ROW_NUMBER() OVER(PARTITION BY query_id) AS filter_number,
				pg_time, 
			    timezone
				
		FROM table_explain,
				json_to_record(table_explain.filters::json) AS filters_srtuct("schema" TEXT,  "values" TEXT[], "field" TEXT, "filter" TEXT, "table" TEXT, "operator" TEXT, "type" TEXT)
		WHERE filters_srtuct."operator" <> '<>'
	), 
	table_with_unnest_filtes AS (
		SELECT 
				fk_db_explain,
				query_id,
				"schema",
				"table",
				"field",
				"operator",
				"type",
				"filter",
				unnest("values") AS filter_value,
				filter_number,
				pg_time, 
				timezone
		FROM table_with_filtes
	) 
	SELECT 
			fk_db_explain,
			query_id,
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
			max(filter_number) over(PARTITION BY query_id) max_filter_number,
			pg_time, 
			timezone
	FROM table_with_unnest_filtes;
	
	DELETE FROM data_lineages_marts.f_query_with_filters AS target 
	USING tt_explain_object AS t
	WHERE target.fk_db_explain=target.fk_db_explain;

	INSERT INTO data_lineages_marts.f_query_with_filters (fk_db_explain, query_id, "schema", "table", field, "type", "operator", filter_value, filter_numranges, filter_tsrange, filter_number, max_filter_number, pg_time, timezone) 
	SELECT fk_db_explain, query_id, "schema", "table", field, "type", "operator", filter_value, filter_numranges, filter_tsrange, filter_number, max_filter_number, pg_time, timezone
	FROM tt_explain_object;

	UPDATE data_lineages_raw.db_explain
		SET is_porcessed = True 
	WHERE is_porcessed = false;
    drop table if exists tt_explain_object;
end;
$$;



CREATE TABLE metadata.etl_info (
    pk_etl_info bigserial NOT NULL PRIMARY KEY,
    process_name text,
    object_name TEXT,
    date_from timestamp,
    created_dttm timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone
);
CREATE UNIQUE INDEX etl_info_process_name_idx ON metadata.etl_info (process_name,object_name, pk_etl_info DESC);

CREATE OR REPLACE function metadata.fn_set_etl_info_last_date_from(
		p_process_name TEXT,
		p_object_name TEXT,
        p_date_from timestamp
    )
RETURNS void
language plpgsql
as
$$
begin
    INSERT INTO metadata.etl_info (process_name, object_name, date_from) VALUES(p_process_name, p_object_name, p_date_from);
end
$$;

CREATE OR REPLACE function metadata.fn_get_etl_info_last_date_from(
		p_process_name TEXT,
		p_object_name TEXT
    )
RETURNS Timestamp 
language plpgsql
as
$$
declare
    var_date_from Timestamp;
BEGIN
	
	SELECT date_from INTO var_date_from
	FROM metadata.etl_info
	WHERE 
		process_name = p_process_name 
		AND  object_name = p_object_name
	ORDER BY pk_etl_info DESC
	LIMIT 1;
    RETURN coalesce(var_date_from, '1990-01-01'::timestamp);
end
$$;

CREATE OR REPLACE function data_lineages_marts.fn_get_filter_numrange_gist(
		p_schema TEXT,
		p_table TEXT,
		p_field TEXT,
		p_lower_bound integer, 
    	p_upper_bound integer,
    	p_granularity integer
    )
returns table (
        numrange_interval TEXT,
        count_query  BIGINT
)
language plpgsql
as
$$
begin
	return Query
	WITH bound_ranges AS (
		SELECT 
			q.bound as lower_bound,
	        lead(q.bound) over (order by bound) as upper_bound
	    FROM (
			SELECT generate_series(p_lower_bound, p_upper_bound, p_granularity)::numeric AS bound
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
				n_r."lower_bound",
				t.query_id,
				count(t.filter_number) over(PARTITION BY n_r."lower_bound", t.query_id) AS count_filtes,
				max(t.max_filter_number) over(PARTITION BY n_r."lower_bound", t.query_id) AS max_filtes,
				n_r."numrange_interval",
				t.filter_numranges
		FROM num_ranges AS n_r
		LEFT JOIN data_lineages_raw.test t
			ON n_r."numrange_interval" && t.filter_numranges
				AND t."schema" = p_schema
				AND t."table" = p_table
				AND t.field = p_field
		)
		
		SELECT 
			t."numrange_interval"::text, 
			count(DISTINCT t.query_id) AS count_query
		FROM count_ranges AS t
		WHERE t.max_filtes = count_filtes
			OR t.max_filtes IS null
		GROUP BY t."numrange_interval";
END 
$$;

