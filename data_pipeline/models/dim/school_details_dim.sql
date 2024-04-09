-- models/dim/school_details_dim.sql
{{ config(materialized='view') }}

SELECT
    {{ generate_school_id('school_name', 'address') }} AS school_id,
    school_name,
    address,
    city,
    school_link,
    review_link,
    ARRAY_TO_STRING(school_types, ',') AS school_types,  -- Convert ARRAY to STRING if needed
    is_prek,
    is_elementary,
    is_middle,
    is_high
FROM {{ source('staging', 'geoencoded_schools_stage') }}
