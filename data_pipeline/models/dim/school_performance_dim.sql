{{ config(materialized='view') }}

SELECT
    MD5(school_name || address) AS school_id,
    gso_rating,
    academic_progress,
    test_scores,
    equity_scores,
    star_rating,
    normalized_test_scores,
    normalized_academic_progress,
    normalized_equity_scores,
    composite_score,
    score_category
FROM {{ source('staging', 'geoencoded_schools_stage') }}
