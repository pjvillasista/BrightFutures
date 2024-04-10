-- models/school_mart.sql
{{ config(materialized='view') }}

with school_base as (
    select
        l.SCHOOL_ID,
        l.SCHOOL_NAME,
        l.ADDRESS,
        l.CITY,
        l.COORDINATES,
        l.LATITUDE,
        l.LONGITUDE,
        d.SCHOOL_LINK,
        d.IS_PREK,
        d.IS_ELEMENTARY,
        d.IS_MIDDLE,
        d.IS_HIGH,
        p.GSO_RATING,
        p.ACADEMIC_PROGRESS,
        p.TEST_SCORES,
        p.EQUITY_SCORES,
        p.SCORE_CATEGORY
    from {{ ref('location_dim') }} l
    join {{ ref('school_details_dim') }} d on l.SCHOOL_ID = d.SCHOOL_ID
    join {{ ref('school_performance_dim') }} p on l.SCHOOL_ID = p.SCHOOL_ID
),
review_summary as (
    select
        SCHOOL_ID,
        avg(POLARITY) as polarity,
        listagg(SENTIMENT, ', ') within group (order by REVIEW_ID) as sentiments, -- Collecting sentiments
        listagg(POSITIVE_HIGHLIGHTS, ' ') within group (order by REVIEW_ID) as positive_highlights,
        listagg(NEGATIVE_HIGHLIGHTS, ' ') within group (order by REVIEW_ID) as negative_highlights
    from {{ ref('reviews_fact') }}
    group by SCHOOL_ID
)



select
    b.SCHOOL_NAME,
    b.ADDRESS,
    b.CITY,
    b.COORDINATES,
    b.LATITUDE,
    b.LONGITUDE,
    b.SCHOOL_LINK,
    b.IS_PREK,
    b.IS_ELEMENTARY,
    b.IS_MIDDLE,
    b.IS_HIGH,
    b.GSO_RATING,
    b.ACADEMIC_PROGRESS,
    b.TEST_SCORES,
    b.EQUITY_SCORES,
    b.SCORE_CATEGORY,
    r.polarity,
    r.sentiments,
    r.positive_highlights,
    r.negative_highlights
from school_base b
left join review_summary r on b.SCHOOL_ID = r.SCHOOL_ID
