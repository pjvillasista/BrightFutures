-- models/fact/reviews_fact.sql
{{
  config(
    materialized = 'table'
    )
}}

-- WITH deduplicated_reviews AS (
--   SELECT
--     *,
--     ROW_NUMBER() OVER (
--       PARTITION BY school_name, address, review
--       ORDER BY polarity DESC
--     ) AS rn
--   FROM 
-- )

-- SELECT
--     {{ dbt_utils.generate_surrogate_key(['school_name', 'address', 'review', 'polarity']) }} AS review_fact_id,
--     {{ generate_school_id('school_name', 'address', 'city','coordinates') }} AS school_id,
--     review,
--     processed_review,
--     polarity,
--     subjectivity,
--     sentiment,
--     positive_highlights,
--     negative_highlights
-- FROM deduplicated_reviews
-- WHERE rn = 1
with source_data as (
    select
        row_number() over (order by (select null)) as review_id, -- Generating a surrogate key
        md5(school_name || '-' || address) as school_id,
        school_name,
        address,
        review,
        polarity,
        subjectivity,
        sentiment,
        positive_highlights,
        negative_highlights
    from {{ source('staging', 'reviews_sentiment_stage') }}
)

select
    review_id,
    school_id,
    school_name,
    address,
    review,
    polarity,
    subjectivity,
    sentiment,
    positive_highlights,
    negative_highlights
from source_data