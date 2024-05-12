select section.section_name, array_agg(distinct keywords.keyword_value) AS keyword_names from {{ source('nyt_db', 'section') }} 
inner join {{ source ('nyt_db', 'fact_nyt') }} on section.section_id = fact_nyt.section_id inner join {{ source ('nyt_db', 'article_keyword') }} 
on fact_nyt.article_id = article_keyword.article_id inner join {{ source ('nyt_db', 'keywords')}} on article_keyword.keyword_id = keywords.keyword_id group by section.section_name
