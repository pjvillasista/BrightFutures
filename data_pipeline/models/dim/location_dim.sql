-- models/dim/location_dim.sql
{{ config(materialized='view') }}

SELECT
    MD5(LOWER(TRIM(address)) || ',' || LOWER(TRIM(city))) AS location_id,
    {{ generate_school_id('school_name', 'address') }} AS school_id,
    LOWER(TRIM(address)) AS address,
    LOWER(TRIM(city)) AS city,
    coordinates,
    latitude,
    longitude
FROM {{ source('staging', 'geoencoded_schools_stage') }}