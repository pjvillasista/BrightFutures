{% macro generate_school_id(school_name, address) %}
    MD5(LOWER(TRIM({{ school_name }})) || LOWER(TRIM({{ address }})))
{% endmacro %}
