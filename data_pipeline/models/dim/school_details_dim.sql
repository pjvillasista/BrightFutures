{{ config(materialized='view') }}

SELECT
    MD5(school_name || address) AS school_id,
    school_name,
    address,
    city,
    school_link,
    review_link,
    school_types::string as school_types,
    is_prek,
    is_elementary,
    is_middle,
    is_high
FROM {{ source('staging', 'geoencoded_schools_stage') }}
