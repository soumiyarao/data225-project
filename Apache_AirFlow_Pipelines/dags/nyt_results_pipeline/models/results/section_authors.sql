with author_table as (
    select *
    from {{source('nyt_db', 'author')}}
),
article_author_table as (
    select *
     from {{source('nyt_db', 'article_author')}}
),
article_table as (
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
ranked_authors AS (
    SELECT
        section.news_desk,
        author.firstname,
        author.lastname,
        COUNT(*) AS num_articles,
        ROW_NUMBER() OVER (PARTITION BY section.news_desk ORDER BY COUNT(*) DESC) AS ranking
    FROM
        section_table AS section 
    JOIN
        fact_nyt_table AS fact_nyt ON fact_nyt.section_id = section.section_id
    JOIN 
        article_author_table AS article_author ON fact_nyt.article_id = article_author.articleid
    JOIN
	    author_table AS author ON article_author.authorid = author.authorid
    WHERE section.news_desk is not null
    GROUP BY
        section.news_desk, author.firstname, author.lastname
)
SELECT 
    news_desk,
    firstname,
    lastname,
    num_articles
FROM 
    ranked_authors
WHERE 
    ranking = 1
ORDER BY num_articles DESC