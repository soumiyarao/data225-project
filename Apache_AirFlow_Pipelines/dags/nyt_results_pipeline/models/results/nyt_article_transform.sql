select section.section_name as section_name, count(fact_nyt.article_id) as article_count from {{ source('nyt_db', 'section') }} as section join {{ source('nyt_db', 'fact_nyt') }} 
as fact_nyt on section.section_id = fact_nyt.section_id group by section.section_name
