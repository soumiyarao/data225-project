from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import numpy as np
from pandas import json_normalize
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
#from airflow.operators.mysql_operator import MySqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def fetch_dailies_from_api():
    url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
    params = {
        "api-key": ""  # Configure your API key
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data

def preprocess():
    response_json = fetch_dailies_from_api()
    parsed_response = json.loads(json.dumps(response_json))
    docs_array = parsed_response['response']['docs']
    articles = json_normalize(docs_array)
    df = pd.DataFrame(articles)
    for col in df.columns:
        print(col)
        
    mysql_hook = MySqlHook(mysql_conn_id='production_nyt', schema='nyt') # Configure appropriate connection details
    connection = mysql_hook.get_conn()

    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM fact_nyt")
    new_row_key = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM author")
    new_author_key = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM keywords")
    new_keyword_key = cursor.fetchone()[0]

    df.replace({np.nan: None}, inplace=True)

    for index, row in df.iterrows():
        new_row_key = new_row_key + 1

        article_info = {
            'abstract': row['abstract'],
            'web_url': row['web_url'],
            'snippet': row['snippet'],
            'lead_paragraph': row['lead_paragraph'],
            'source': row['source'],
            'pub_date': row['pub_date'],
            'document_type': row['document_type'],
            'type_of_material': row['type_of_material'],
            '_id': row['_id'],
            'word_count': row['word_count'],
            'uri': row['uri'],
            'print_section': row['print_section'],
            'print_page': row['print_page']
        }

        section_info = {
            'section_name': row['section_name'],
            'subsection_name': row['subsection_name'],
            'news_desk': row['news_desk']
        }

        headline_info = {
            'main': row['headline.main'] if row['headline.main'] is not None else '',
            'kicker': row['headline.kicker'] if row['headline.kicker'] is not None else '',
            'content_kicker': row['headline.content_kicker'] if row['headline.content_kicker'] is not None else '',
            'print_headline': row['headline.print_headline'] if row['headline.print_headline'] is not None else '',
            'name': row['headline.name'] if row['headline.name'] is not None else '',
            'seo': row['headline.seo'] if row['headline.seo'] is not None else '',
            'sub': row['headline.sub'] if row['headline.sub'] is not None else ''
        }

        keyword_info = []
        for keyword in row['keywords']:
            keyword_dict = {
                'keyword_name': keyword['name'] if keyword.get('name') is not None else '',
                'keyword_value': keyword['value'] if keyword.get('value') is not None else '',
                'keyword_rank': keyword['rank'] if keyword.get('rank') is not None else '',
                'major': keyword['major'] if keyword.get('major') is not None else ''
            }
            keyword_info.append(keyword_dict)

        author_info = []

        for person in row['byline.person']:
            person_dict = {
                'rank': person['rank'],
                'firstname': person['firstname'] if person.get('firstname') is not None else '',
                'middlename': person['middlename'] if person.get('middlename') is not None else '',
                'lastname': person['lastname'] if person.get('lastname') is not None else '',
            }
            author_info.append(person_dict)

        sql = "select * from article where _id = %s"
        cursor.execute(sql, (article_info['_id'],))
        result = cursor.fetchone()

        if result:
            print("Row present. Continuing...")
            continue
        else:
            print("Proceeding...")

        print(section_info)
        check_sql = "select * from section where section_name = %s and subsection_name = %s and news_desk = %s"
        check_values = (section_info['section_name'], section_info['subsection_name'], section_info['news_desk'])
        cursor.execute(check_sql, check_values)
        section_exists = cursor.fetchone()
        section_id_to_insert = new_row_key
        if section_exists:
            print("Section already exists.")
            section_id_to_insert = section_exists[0]


        fact_sql = "INSERT INTO fact_nyt (article_id, section_id, highlight_id) VALUES (%s, %s, %s)"
        fact_values = (article_info['_id'], section_id_to_insert, new_row_key)
        cursor.execute(fact_sql, fact_values)
        connection.commit()
        print("Commit fact")


        article_sql = "INSERT INTO article (snippet, web_url, print_section, uri, source, document_type, type_of_material, lead_paragraph, abstract, _id, print_page, word_count, pub_date, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        article_values = (
        article_info['snippet'], article_info['web_url'], article_info['print_section'], article_info['uri'],
        article_info['source'], article_info['document_type'], article_info['type_of_material'],
        article_info['lead_paragraph'], article_info['abstract'], article_info['_id'], article_info['print_page'],
        article_info['word_count'], article_info['pub_date'], datetime.now())
        cursor.execute(article_sql, article_values)
        connection.commit()
        print("Commit art")

        print(section_info)
        if not section_exists:
            print("Not present. Adding new sec...")
            section_sql = "INSERT INTO section (section_id, section_name, subsection_name, news_desk, timestamp) VALUES (%s, %s, %s, %s, %s)"
            section_values = (section_id_to_insert, section_info['section_name'], section_info['subsection_name'], section_info['news_desk'], datetime.now())
            cursor.execute(section_sql, section_values)
            connection.commit()
            print("Commit sec")

        print(headline_info)
        headline_sql = "INSERT INTO highlights (highlight_id, main, kicker, content_kicker, print_headline, name, seo, sub, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        headline_values = (new_row_key, headline_info['main'], headline_info['kicker'], headline_info['content_kicker'],
                           headline_info['print_headline'], headline_info['name'], headline_info['seo'],
                           headline_info['sub'], datetime.now())
        headline_values = tuple(str(value) if value is not None else '' for value in headline_values)
        cursor.execute(headline_sql, headline_values)
        connection.commit()
        print("Commit highlights")

        for keyword_entry in keyword_info:
            check_sql = "select * from keywords where keyword_name = %s and keyword_value = %s and keyword_rank = %s and major = %s"
            check_values = (keyword_entry['keyword_name'], keyword_entry['keyword_value'], keyword_entry['keyword_rank'], keyword_entry['major'])
            cursor.execute(check_sql, check_values)
            keyword_exists = cursor.fetchone()
            print(keyword_exists)
            if keyword_exists:
                keyword_key_for_row = keyword_exists[0]
                sql = "INSERT INTO article_keyword (article_id, keyword_id, timestamp) VALUES (%s, %s, %s)"
                values = (article_info['_id'], keyword_key_for_row, datetime.now())
                cursor.execute(sql, values)
                connection.commit()
                print("Old keyword commit")
            else:
                print("New keyword commit")
                new_keyword_key = new_keyword_key + 1;
                keyword_key_for_row = new_keyword_key
                sql = "insert into keywords (keyword_id, keyword_name, keyword_value, keyword_rank, major, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
                values = (
                    keyword_key_for_row, keyword_entry['keyword_name'], keyword_entry['keyword_value'], keyword_entry['keyword_rank'], keyword_entry['major'],
                    datetime.now()
                )
                cursor.execute(sql, values)
                connection.commit()
                sql = "INSERT INTO article_keyword (article_id, keyword_id, timestamp) VALUES (%s, %s, %s)"
                values = (article_info['_id'], keyword_key_for_row, datetime.now())
                cursor.execute(sql, values)
                connection.commit()

        for author_entry in author_info:
            check_sql = "select * from author where firstname = %s and middlename = %s and lastname = %s"
            check_values = (author_entry['firstname'], author_entry['middlename'], author_entry['lastname'])
            cursor.execute(check_sql, check_values)
            author_exists = cursor.fetchone()
            if author_exists:
                author_key_for_row = author_exists[0]
                sql = "INSERT INTO article_author (articleid, authorid, ranking, timestamp) VALUES (%s, %s, %s, %s)"
                values = (article_info['_id'], author_key_for_row, author_entry['rank'], datetime.now())
                cursor.execute(sql, values)
                connection.commit()
                print("Old author commit")
            else:
                print("New author commit")
                new_author_key = new_author_key + 1;
                author_key_for_row = new_author_key
                sql = "insert into author (authorid, firstname, middlename, lastname, timestamp) VALUES (%s, %s, %s, %s, %s)"
                values = (
                    author_key_for_row, author_entry['firstname'], author_entry['middlename'], author_entry['lastname'], datetime.now()
                )
                cursor.execute(sql, values)
                connection.commit()
                sql = "INSERT INTO article_author (articleid, authorid, ranking, timestamp) VALUES (%s, %s, %s, %s)"
                values = (article_info['_id'], author_key_for_row, author_entry['rank'], datetime.now())
                cursor.execute(sql, values)
                connection.commit()

    cursor.close()
    connection.close()

with DAG(
    'archive_api_dag',
    default_args=default_args,
    description='Fetch data from New York Times API and load into Snowflake',
    schedule_interval='@daily',
) as dag:

    extract_dailies = PythonOperator(
        task_id='extract_dailies',
        python_callable=fetch_dailies_from_api,
    )

    transform_and_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=preprocess,
    )

extract_dailies >> transform_and_load
