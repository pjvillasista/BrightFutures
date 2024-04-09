{{ config(materialized='table') }}

WITH school_details AS (
    SELECT
        school_id,
        school_name,
        address,
        city,
        school_link,
        is_prek,
        is_elementary,
        is_middle,
        is_high
    FROM {{ ref('school_details_dim') }}
),

school_location AS (
    SELECT
        school_id,
        address AS location_address,
        city AS location_city,
        coordinates,
        latitude,
        longitude
    FROM {{ ref('location_dim') }}
),

school_performance AS (
    SELECT
        school_id,
        gso_rating,
        academic_progress,
        test_scores,
        equity_scores,
        star_rating,
        composite_score,
        score_category
    FROM {{ ref('school_performance_dim') }}
),

review_facts AS (
    SELECT
        school_id,
        review_fact_id,
        polarity,
        sentiment,
        positive_highlights,
        negative_highlights
    FROM {{ ref('reviews_fact') }}
)

SELECT
    sd.school_name,
    sl.location_address AS address,
    sl.location_city AS city,
    sl.coordinates,
    sl.latitude,
    sl.longitude,
    sd.school_link,
    sd.is_prek,
    sd.is_elementary,
    sd.is_middle,
    sd.is_high,
    sp.gso_rating,
    sp.academic_progress,
    sp.test_scores,
    sp.equity_scores,
    sp.star_rating,
    sp.composite_score,
    sp.score_category,
    rf.polarity,
    rf.sentiment,
    rf.positive_highlights,
    rf.negative_highlights
FROM school_details sd
LEFT JOIN school_location sl ON sd.school_id = sl.school_id
LEFT JOIN school_performance sp ON sd.school_id = sp.school_id
LEFT JOIN review_facts rf ON sd.school_id = rf.school_id
