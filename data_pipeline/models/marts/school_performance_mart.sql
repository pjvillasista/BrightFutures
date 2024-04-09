{{ config(materialized='table') }}

WITH school_details AS (
    SELECT * FROM {{ ref('school_details_dim') }}
),

school_location AS (
    SELECT * FROM {{ ref('location_dim') }}
),

review_facts AS (
    SELECT * FROM {{ ref('reviews_fact') }}
)

SELECT
    sd.school_name,
    sl.address,
    sl.city,
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
    r.highlights,
    r.polarity,
    r.sentiment,
    r.positive_highlights,
    r.negative_highlights
FROM school_details sd
JOIN school_location sl ON sd.location_id = sl.location_id
JOIN school_performance sp ON sd.school_id = sp.school_id
LEFT JOIN reviews r ON sd.school_id = r.school_id
