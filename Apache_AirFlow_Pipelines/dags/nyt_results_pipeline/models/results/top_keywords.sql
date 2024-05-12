WITH RankedKeywords AS (
  SELECT keyword_name, keyword_value,
         RANK() OVER (PARTITION BY keyword_name ORDER BY COUNT(*) DESC) AS Rank_keywords
  FROM {{ source ('nyt_db', 'keywords')}}
  GROUP BY keyword_name, keyword_value
)
SELECT keyword_name, keyword_value
FROM RankedKeywords
WHERE Rank_keywords <= 5
