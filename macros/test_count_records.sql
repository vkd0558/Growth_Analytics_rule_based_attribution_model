
{% macro count_records(query, expect) %}
  {% set result = run_query(query) %}
  {% set row_count = result.row_count %}
  
  {% if row_count == expect.row_count %}
    {% do log('Records count test passed: ' ~ row_count) %}
  {% else %}
    {% do log('Records count test failed: expected ' ~ expect.row_count ~ ', actual ' ~ row_count, level='error') %}
  {% endif %}
{% endmacro %}



