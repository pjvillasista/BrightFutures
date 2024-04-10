-- models/dim/school_details_dim.sql
{{ config(materialized='view') }}

-- SELECT
--     {{ generate_school_id('school_name', 'address', 'city','coordinates') }} AS school_id,
--     school_name,
--     address,
--     city,
--     school_link,
--     review_link,
--     ARRAY_TO_STRING(school_types, ',') AS school_types,  -- Convert ARRAY to STRING if needed
--     is_prek,
--     is_elementary,
--     is_middle,
--     is_high
-- FROM {{ source('staging', 'geoencoded_schools_stage') }}

with source_data as (
    select distinct
        md5(school_name || '-' || address) as school_id,
        school_name,
        address,
        school_link,
        school_types,
        is_prek,
        is_elementary,
        is_middle,
        is_high
    from {{ source('staging', 'geoencoded_schools_stage') }}
)

select
    school_id,
    school_name,
    address,
    school_link,
    school_types,
    is_prek,
    is_elementary,
    is_middle,
    is_high
from source_data