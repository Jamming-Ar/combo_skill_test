{% macro normalize_date(column_name) %}
    coalesce(
        safe.parse_date('%Y-%m-%d',  cast({{ column_name }} as string)),
        safe.parse_date('%d/%m/%Y',  cast({{ column_name }} as string)),
        safe.parse_date('%m/%d/%Y',  cast({{ column_name }} as string)),
        safe.parse_date('%Y/%m/%d',  cast({{ column_name }} as string)),
        safe.parse_date('%d-%m-%Y',  cast({{ column_name }} as string)),
        safe.parse_date('%Y%m%d',    cast({{ column_name }} as string))
    )
{% endmacro %}