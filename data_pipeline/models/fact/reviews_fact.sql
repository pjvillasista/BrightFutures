{{
  config(
    materialized = 'incremental',
    unique_key='review_fact_id',
    on_schema_change='append_new_columns'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['school_name', 'address', 'review']) }} AS review_fact_id,
    MD5(school_name || address) AS school_id, -- Foreign key to `school_details_dim`
    review,
    processed_review,
    polarity,
    subjectivity,
    sentiment,
    positive_highlights,
    negative_highlights
FROM {{ source('staging', 'reviews_sentiment_stage') }}
