{% macro calculate_expected_location_revenue(employee_count) %}
case
    when {{ employee_count }} <= 5
        then 60.0
    when {{ employee_count }} <= 39
        then 80.0 + ({{ employee_count }} - 6) * 4.0
    else
        216.0
        + (33 * 4.0)
        + ({{ employee_count }} - 39) * 2.4
end
{% endmacro %}
