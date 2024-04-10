{% macro generate_school_id(school_name, address, city, coordinates) %}
    MD5(
        LOWER(TRIM({{ school_name }})) || 
        LOWER(TRIM({{ address }})) || 
        LOWER(TRIM({{ city }})) || 
        LOWER(TRIM({{ coordinates }}))
    )
{% endmacro %}
