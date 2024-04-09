-- models/fact/reviews_fact.sql
{{
  config(
    materialized = 'incremental',
    unique_key='review_fact_id',
    on_schema_change='append_new_columns'
    )
}}

WITH deduplicated_reviews AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY school_name, address, review
      ORDER BY polarity DESC
    ) AS rn
  FROM {{ source('staging', 'reviews_sentiment_stage') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['school_name', 'address', 'review', 'polarity']) }} AS review_fact_id,
    {{ generate_school_id('school_name', 'address') }} AS school_id,
    review,
    processed_review,
    polarity,
    subjectivity,
    sentiment,
    positive_highlights,
    negative_highlights
FROM deduplicated_reviews
WHERE rn = 1
