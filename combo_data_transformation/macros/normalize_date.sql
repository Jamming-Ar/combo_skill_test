{% macro normalize_date(column_name) %}
    case
        when {{ column_name }} like '__/__/____'
            then parse_date('%d/%m/%Y', {{ column_name }})
        else
            parse_date('%Y-%m-%d', {{ column_name }})
    end
{% endmacro %}