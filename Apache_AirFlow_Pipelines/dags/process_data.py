import requests
import pandas as pd
from pandas import json_normalize
import snowflake.connector
from urllib.parse import quote_plus
from datetime import datetime
from sqlalchemy import create_engine
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta


snowflake_connection_params = {
    'user': '***',
    'password': '***',
    'account': '***',
    'warehouse': 'nyt_wh',
    'database': 'nyt_db',
    'schema': 'nyt_schema'
}

def fetch_nytimes_data():
    
    current_date = datetime.now()
    previous_month_date = current_date - relativedelta(months=1)
    year = previous_month_date.year
    month = previous_month_date.month
    url = f'https://api.nytimes.com/svc/archive/v1/{year}/{month}.json'
    api_key = '3ocLG9fAGohMCKWgJtGxvXitqY5ZJHPp'
    
    params = {'api-key': api_key}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        print('API request successful')
        data = response.json()
        # Process the data as needed
        articles = data['response']['docs']
        return articles
    else:
        print('API request failed')
        print(response.text)  


def preprocessing():
    articles = fetch_nytimes_data()
    #Preprocessing articles
    df = pd.DataFrame(articles)
    df_normalized = pd.json_normalize(articles)
    df.drop('multimedia', axis=1, inplace=True)
    articles_df = df[['abstract', 'web_url','snippet','lead_paragraph','source','pub_date','document_type','section_name', 'news_desk', 'type_of_material','_id','word_count','uri']]
    articles_df['pub_date'] = pd.to_datetime(articles_df['pub_date']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    #Preprocessing headline table
    headline_df = df[['headline','_id']]
    final_headline_df = json_normalize(headline_df['headline'])
    headline_df1 = pd.concat([headline_df['_id'], final_headline_df], axis=1)
    
    #Preprocessing keywords
    keywords_df = df[['keywords','_id']]
    keywords_df = df[['keywords','_id']]
    dfs_with_id = []
    for index, row in keywords_df.iterrows():
        keywords = row['keywords']
        if keywords: 
            keywords_df = pd.DataFrame(keywords)  
        else:
            keywords_df = pd.DataFrame(columns=['name', 'value', 'rank', 'major'])
        keywords_df['_id'] = row['_id']
        dfs_with_id.append(keywords_df)
    final_keywords_df = pd.concat(dfs_with_id, ignore_index=True)
    
    #Preprocessing Byline
    normalized_byline_list = []
    for item in articles:
        byline_data = item['byline']
        article_id = item['_id']

        for person in byline_data.get("person", []):
            normalized_data = {
                "firstname": person.get("firstname", ""),
                "middlename": person.get("middlename", ""),
                "lastname": person.get("lastname", ""),
                "qualifier": person.get("qualifier", None),
                "title": person.get("title", None),
                "role": person.get("role", ""),
                "organization": person.get("organization", ""),
                "rank": person.get("rank", 0),
                "original": byline_data.get("original", ""),
                "organization_byline": byline_data.get("organization", ""),
                "_id": article_id 
            }
            normalized_byline_list.append(normalized_data)
    byline_df = pd.DataFrame(normalized_byline_list)
    return articles_df, byline_df, final_keywords_df, headline_df1
                

def insert_data(articles_df, author_df, keywords_df, highlights_df):
    
    print("Inside insert_data")
    try:
        conn = snowflake.connector.connect(**snowflake_connection_params)
        encoded_password = quote_plus(snowflake_connection_params['password'])

        snowflake_connection_string = f'snowflake://{snowflake_connection_params["user"]}:{encoded_password}@{snowflake_connection_params["account"]}/{snowflake_connection_params["database"]}/{snowflake_connection_params["schema"]}?warehouse={snowflake_connection_params["warehouse"]}'
        engine = create_engine(snowflake_connection_string)

        # Execute SQL query to count existing articles and fetch existing _id from ARTICLE table
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM article")
        result = cursor.fetchone()
        num_rows = result[0]
        print(num_rows)
        
        cursor.execute("SELECT _id FROM article")
        existing_ids = [row[0] for row in cursor]
        # Print existing _id values
        print(existing_ids[1])
        
        
        existing_ids_series = pd.Series(existing_ids)
        new_articles_df = articles_df[~articles_df['_id'].isin(existing_ids_series)]
        
        new_articles_df.drop(columns=['section_name', 'news_desk'], inplace = True)
        
        if not new_articles_df.empty:
            new_articles_df.to_sql(name='article', con=engine, if_exists='append', index=False)
            print(f"{len(new_articles_df)} new articles inserted.")
        else:
            print("No new data")
            return
            
        insert_new_authors_to_snowflake(conn, engine, author_df)
        insert_new_keywords_to_snowflake(conn, engine, keywords_df)
        insert_new_section_highlights_fact_to_snowflake(conn, engine, articles_df, highlights_df)
            
    except snowflake.connector.errors.DatabaseError as e:
        print(f"Snowflake database error: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
    
def insert_new_authors_to_snowflake(conn, engine, authors_df):
    
    print("Inside Authors")

    df = authors_df

    author_df = pd.DataFrame(columns=['authorid', 'firstname', 'lastname', 'middlename'])
    article_author_df = pd.DataFrame(columns=['authorid', 'articleid', 'ranking'])

    query = "SELECT authorid, firstname, middlename, lastname FROM author"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    author_dict = {}
    for result in results:
        authorid = result[0]
        firstname = result[1]
        middlename = result[2]
        lastname = result[3]
        author_dict[(firstname,middlename, lastname)] = authorid

    # Check if the author already exists in the Snowflake table
    for index, row in df.iterrows():
        if (row['firstname'], row['middlename'], row['lastname']) in author_dict:
            author_id = author_dict[(row['firstname'], row['middlename'], row['lastname'])]
        else:
            author_id = str(uuid.uuid4())
            author_df = author_df._append({'authorid': author_id, 'firstname': row['firstname'], 'lastname': row['lastname'], 'middlename': row['middlename']}, ignore_index=True)
            author_dict[(row['firstname'], row['middlename'], row['lastname'])] = authorid
        
        # Append the author_id to article_author_df
        article_author_df = article_author_df._append({'authorid': author_id, 'articleid': row['_id'], 'ranking': row['rank']}, ignore_index=True)
            

    # Now, you can insert both DataFrames into Snowflake
    author_df.to_sql(name='author', con=engine, if_exists='append', index=False)
    article_author_df.to_sql(name='article_author', con=engine, if_exists='append', index=False)
    
    print("Successfull")


    
def insert_new_keywords_to_snowflake(conn, engine, keywords_df):
    
    print("Inside Keywords")
    df = keywords_df

    keywords_df = pd.DataFrame(columns=['keyword_id', 'keyword_value', 'keyword_name', 'major'])
    article_keywords_df = pd.DataFrame(columns=['article_id', 'keyword_id'])

    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_connection_params)

    query = "SELECT keyword_id, keyword_value, keyword_name FROM keywords"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    query_id = "SELECT keyword_id from keywords order by keyword_id desc"
    cursor = conn.cursor()
    cursor.execute(query_id)
    result  = cursor.fetchone()
    id = result[0]
    print(id)

    keyword_dict = {}
    for result in results:
        keyword_id = result[0]
        keyword_value = result[1]
        keyword_name = result[2]
        keyword_dict[(keyword_value,keyword_name)] = keyword_id

    # Check if the author already exists in the Snowflake table
    for index, row in df.iterrows():
        if (row['value'], row['name']) in keyword_dict:
            keyword_id = keyword_dict[(row['value'], row['name'])]
        else:
            id = id + 1
            keyword_id = id
            keywords_df = keywords_df._append({'keyword_id': keyword_id, 'keyword_value': row['value'], 'keyword_name': row['name'], 'major': row['major']}, ignore_index=True)
            keyword_dict[(row['value'], row['name'])] = keyword_id
        
        # Append the author_id to article_author_df
        article_keywords_df = article_keywords_df._append({'keyword_id': keyword_id, 'article_id': row['_id']}, ignore_index=True)
            

    # Now, you can insert both DataFrames into Snowflake
    keywords_df.to_sql(name='keywords', con=engine, if_exists='append', index=False)
    article_keywords_df.to_sql(name='article_keyword', con=engine, if_exists='append', index=False)

    print("Successfull")
    

def insert_new_section_highlights_fact_to_snowflake(conn, engine, article_df, highlights_df):
    
    print("Inside section_highlights_fact")
    
    section_df = pd.DataFrame(columns=['section_id', 'section_name', 'news_desk'])
    fact_nyt_df = pd.DataFrame(columns=['article_id', 'section_id', 'highlight_id'])
    #highlights_df = pd.DataFrame(columns=['highlight_id', 'main', 'sub', 'name', 'kicker', 'print_headline', 'seo', 'content_kicker'])

    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_connection_params)

    query = "SELECT section_id, section_name, news_desk FROM section"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    query_id = "SELECT section_id from section order by section_id desc"
    cursor = conn.cursor()
    cursor.execute(query_id)
    result  = cursor.fetchone()
    id = result[0]
    print(id)

    query_id_1 = "SELECT highlight_id from highlights order by highlight_id desc"
    cursor = conn.cursor()
    cursor.execute(query_id_1)
    result  = cursor.fetchone()
    h_id = result[0]
    print(h_id)

    article_highlights_dict = {}
    for index, row in highlights_df.iterrows():
        h_id = h_id + 1
        article_highlights_dict[row['_id']] = h_id
        highlights_df.at[index, 'highlight_id'] = h_id

    highlights_df.drop(columns=['_id'], inplace=True)
    print(highlights_df.head(5))
        
    section_dict = {}
    for result in results:
        section_id = result[0]
        section_name = result[1]
        news_desk = result[2]
        section_dict[(section_name,news_desk)] = section_id

    # Check if the author already exists in the Snowflake table
    for index, row in article_df.iterrows():
        if (row['section_name'], row['news_desk']) in section_dict:
            section_id = section_dict[(row['section_name'], row['news_desk'])]
        else:
            id = id + 1
            section_id = id
            section_df = section_df._append({'section_id': section_id, 'section_name': row['section_name'], 'news_desk': row['news_desk']}, ignore_index=True)
            section_dict[(row['section_name'], row['news_desk'])] = section_id
        
        # Append the author_id to article_author_df
        fact_nyt_df = fact_nyt_df._append({'section_id': section_id, 'article_id': row['_id'], 'highlight_id': article_highlights_dict[row['_id']]}, ignore_index=True)
        
    print(fact_nyt_df.head(5))

    # Now, you can insert both DataFrames into Snowflake
    section_df.to_sql(name='section', con=engine, if_exists='append', index=False)
    highlights_df.to_sql(name='highlights', con=engine, if_exists='append', index=False)
    fact_nyt_df.to_sql(name='fact_nyt', con=engine, if_exists='append', index=False)

    print("Successfull")