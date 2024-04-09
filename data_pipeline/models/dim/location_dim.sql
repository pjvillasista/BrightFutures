{{ config(materialized='view') }}

SELECT
    MD5(address || city) AS location_id,
    address,
    city,
    coordinates,
    latitude,
    longitude
FROM {{ source('staging', 'geoencoded_schools_stage') }}