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
)
SELECT
    author_table.firstname,
	author_table.lastname,
    COUNT(article_author_table.articleid) AS num_articles,
    AVG(article_table.word_count) AS avg_word_count
FROM
    author_table
JOIN
    article_author_table ON author_table.authorid = article_author_table.authorid
JOIN
    article_table ON article_author_table.articleid = article_table._id
GROUP BY
    author_table.authorid, author_table.firstname, author_table.lastname


