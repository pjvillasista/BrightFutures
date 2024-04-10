-- models/dim/location_dim.sql
{{ config(materialized='view') }}

with source_data as(
    select distinct
        MD5(school_name || '-' || address) as school_id,
        school_name,
        address,
        city,
        coordinates,
        latitude,
        longitude
    from {{ source('staging', 'geoencoded_schools_stage') }}
)
select
    school_id,
    school_name,
    address,
    city,
    coordinates,
    latitude,
    longitude
from source_data