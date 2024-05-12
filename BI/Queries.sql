#Query 1: Regioned Based Query:
SELECT
SUM(CASE WHEN abstract LIKE '%New York%' OR abstract LIKE '%America%'
THEN 1 ELSE 0 END) AS America_count,
SUM(CASE WHEN abstract LIKE '%Asia%' OR abstract LIKE '%China%' OR
abstract LIKE '%Europe%' OR abstract LIKE '%Africa%'
OR abstract LIKE '%Australia%' OR abstract LIKE '%Russia%' OR abstract LIKE
'%India%' OR abstract LIKE '%London %' OR abstract LIKE '%Ukraine%'
OR abstract LIKE '%Israel%' THEN 1 ELSE 0 END) AS Other_Continents_count
FROM  article;

#Query 2: Number of articles by Month:
SELECT
    DATE_FORMAT(pub_date, '%Y-%m') AS publication_month,
    COUNT(*) AS num_articles
FROM
    article
GROUP BY
    publication_month
ORDER BY
    publication_month;

#Query 3: Top 5 Keywords by group
WITH RankedKeywords AS (
  SELECT keyword_name, keyword_value,
         RANK() OVER (PARTITION BY keyword_name ORDER BY COUNT(*) DESC) AS Rank_keywords
  FROM keywords
  GROUP BY keyword_name, keyword_value
)
SELECT keyword_name, keyword_value
FROM RankedKeywords
WHERE Rank_keywords <= 5;

#Query 4: Number_of_articles by Type_of_material
SELECT
    type_of_material,
    COUNT(*) AS num_articles
FROM
    article
GROUP BY
    type_of_material;

#Query 5:Number_of_articles belong to each section
select section.section_name as section_name, count(fact_nyt.article_id) as
article_count from SECTION as section join 
FACT_NYT as fact_nyt on section.section_id = fact_nyt.section_id group by section.section_name;


#Query 6:Distribution of keywords across different sections of articles
select section.section_name, keywords.keyword_value AS
keyword_names from SECTION
inner join FACT_NYT ON section.section_id =
fact_nyt.section_id inner join ARTICLE_KEYWORD
on fact_nyt.article_id = article_keyword.article_id inner join 
KEYWORDS on article_keyword.keyword_id = keywords.keyword_id group by
section.section_name;

