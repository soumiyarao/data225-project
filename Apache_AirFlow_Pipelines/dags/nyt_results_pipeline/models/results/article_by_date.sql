SELECT
    TO_CHAR(pub_date, 'YYYY-MM') AS publication_month,
    COUNT(*) AS num_articles
FROM
    {{ source ('nyt_db', 'article') }} 
GROUP BY
    publication_month
ORDER BY
    publication_month
