SELECT 
    SUM(CASE WHEN abstract LIKE '%New York%' OR abstract LIKE '%America%' THEN 1 ELSE 0 END) AS America_count,
    SUM(CASE WHEN abstract LIKE '%Asia%' OR abstract LIKE '%China%' OR abstract LIKE '%Europe%' OR abstract LIKE '%Africa%' 
    OR abstract LIKE '%Australia%' OR abstract LIKE '%Russia%' OR abstract LIKE '%India%' OR abstract LIKE '%London %' OR abstract LIKE '%Ukraine%' 
    OR abstract LIKE '%Israel%' THEN 1 ELSE 0 END) AS Other_Continents_count
FROM  {{ source ('nyt_db', 'article') }}
