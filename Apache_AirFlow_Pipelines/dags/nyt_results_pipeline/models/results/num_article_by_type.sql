SELECT
    type_of_material,
    COUNT(*) AS num_articles
FROM
    {{ source ('nyt_db', 'article') }} 
GROUP BY
    type_of_material
