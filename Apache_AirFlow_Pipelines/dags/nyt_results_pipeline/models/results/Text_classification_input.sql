-- Depends on: {{ ref('section_authors') }}
-- Depends on: {{ ref('article_by_date') }}
-- Depends on: {{ ref('top_keywords') }}
-- Depends on: {{ ref('author_results') }}
-- Depends on: {{ ref('nyt_article_transform') }}
-- Depends on: {{ ref('nyt_section_keywords_transform') }}
-- Depends on: {{ ref('num_article_by_type') }}
-- Depends on: {{ ref('nyt_demographics') }}


with article_table as (
    select *
     from {{source('nyt_db', 'article')}}
),
section_table as (
    select *
    from {{source('nyt_db', 'section')}}
),
fact_nyt_table as (
    select *
     from {{source('nyt_db', 'fact_nyt')}}
),
SectionCounts AS (
    SELECT
        distinct article.abstract,
        article.lead_paragraph,
        section.news_desk,
        section.section_name,
        ROW_NUMBER() OVER (PARTITION BY section.section_name order by article.abstract) AS row_num
    FROM
        section_table as section  
    INNER JOIN
        fact_nyt_table as fact_nyt ON fact_nyt.section_id = section.section_id
    Inner JOIN 
        article_table as article ON article._id = fact_nyt.article_id
)
SELECT
    abstract,
    section_name,
    lead_paragraph,
    news_desk
FROM
    SectionCounts
WHERE
    row_num >= 500
