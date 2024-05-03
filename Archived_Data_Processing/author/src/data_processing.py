import uuid
import pandas as pd
import constants
import db_manager
import ast
import numpy as np
    

def get_dataset(file_name, nrows=None, low_memory=False):
    file = f'{constants.DATA_SET_PATH}/{file_name}.{constants.DATA_SET_EXTENSION}'
    # For windows use \\ as filepath delimeter
    #file = f'{constants.DATA_SET_PATH}\\{file_name}.{constants.DATA_SET_EXTENSION}'
    df = pd.read_csv(filepath_or_buffer=file, nrows=nrows, low_memory=low_memory)
    return df

def remove_duplicates(df, headers=None):
    df_processed = None
    if headers == None:
        df_processed = df.drop_duplicates()
        return df_processed
    
    for header in headers:
        df_processed = df.drop_duplicates(subset=[header])
    return df_processed


def remove_nullrows(df, headers=None):
    df_processed = None
    if headers == None:
        df_processed = df.dropna()
        return df_processed
    
    for header in headers:
        df_processed = df.dropna(subset=[header])
    return df_processed

def replace_non_integer(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return np.nan

def clean_df(df, headers=None):
    df_processed = None
    df_processed = remove_duplicates(df, headers)
    df_processed = remove_nullrows(df_processed, headers)
    return df_processed



def load_articles(host, user,password):
    df_articles = get_dataset(file_name=constants.NYT)#, nrows=5)
  
    df_articles = clean_df(df_articles, headers=constants.article_headers)
    
    df_articles = df_articles.replace(np.nan, None)
    
    #prepare_article_table(df_articles, host, user, password, constants.DATABASE_NAME)

    prepare_parent_and_connecting_tables(df_articles, host, user, password, constants.DATABASE_NAME)
 

    

    
def prepare_article_table(df, host, user, password, database):
    
    
    table_name = constants.ARTICLE_TABLE
    headers = ['_id','section_name']
    
    create_table_query = f"""
        CREATE TABLE {table_name} (
        _id VARCHAR(255) PRIMARY KEY,
        section_name VARCHAR(225)
    )
    """
    
    insert_table_query = f"""
            INSERT INTO {table_name}  
            (_id, section_name) 
            VALUES (%s, %s)
        """

    db_manager.create_insert_table(df, host, user, password, database, table_name, create_table_query, headers, insert_table_query)
     
        
        
        
def prepare_parent_and_connecting_tables(df, host, user, password, database):

    parent_table_data, connecting_table_data = prepare_parent_and_connecting_data(df, 'byline')

    
    table_name = constants.AUTHOR_TABLE
    headers = ['authorid','firstname','lastname','middlename']
    
    create_table_query = f"""
        CREATE TABLE {table_name} (
        authorid VARCHAR(255) PRIMARY KEY,
        firstname VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        lastname VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
        middlename VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    )
    """
    
    insert_table_query = f"""
            INSERT INTO {table_name}  
            (authorid, firstname, lastname, middlename) 
            VALUES (%s, %s, %s, %s)
        """

    db_manager.create_insert_table(parent_table_data, host, user, password, database, table_name, create_table_query, headers, insert_table_query)
    
    
    table_name = constants.ARTICLE_AUTHOR_TABLE
    headers = ['articleid','authorid','rank']
    
    create_table_query = f"""
        CREATE TABLE {table_name} (
        articleid VARCHAR(255),
        authorid VARCHAR(255),
        ranking int,
        Foreign Key (articleid) references {constants.ARTICLE_TABLE}(_id),
        Foreign Key (authorid) references {constants.AUTHOR_TABLE}(authorid),
        Primary Key (articleid, authorid)
    )
    """
    
    insert_table_query = f"""
            INSERT INTO {table_name}  
            (articleid, authorid, ranking) 
            VALUES (%s, %s, %s)
        """

    db_manager.create_insert_table(connecting_table_data, host, user, password, database, table_name, create_table_query, headers, insert_table_query)
    





def prepare_parent_and_connecting_data(df, column_name):

    df_authors = pd.DataFrame(columns=['firstname', 'middlename','lastname', 'authorid'])
    
    df_article_authors = pd.DataFrame(columns=['articleid', 'authorid','rank'])

    unique_authors = set()
    
    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        json_array = []
        person_array = []
        try:
            # Load JSON data from the specified column
            json_object = ast.literal_eval(row[column_name])
            if type(json_object) == dict:
                json_array = [json_object]
            elif type(json_object) == list:
                json_array = json_object
            else:
                print(f'Unsupported json object found: {type(json_object)} in column {column_name}')
        except SyntaxError as e:
            pass
        except ValueError as e:
            pass
        except Exception as e:
            pass
        if(len(json_array)) > 0:
            person_array = json_array[0]['person']
        
            if(len(person_array)) == 0:
                pass
            else:
                for entry in person_array:
                    firstname = entry['firstname']
                    middlename = entry['middlename']
                    lastname = entry['lastname']
                    author_id = str(uuid.uuid4())  
        
                    if (firstname, middlename, lastname) not in unique_authors:
                        if firstname and lastname:
                            df_authors = df_authors._append({'firstname': firstname, 'middlename':middlename, 'lastname': lastname, 'authorid': author_id}, ignore_index=True)
                            unique_authors.add((firstname, middlename, lastname))
                            df_article_authors = df_article_authors._append({'articleid': row['_id'], 'authorid':author_id, 'rank':  entry['rank']}, ignore_index=True)
                    else:
                        if firstname and lastname:
                            author_id = df_authors.loc[
                            (df_authors['firstname'] == firstname) &
                            (df_authors['lastname'] == lastname) , 'authorid'].values[0]
                            df_article_authors = df_article_authors._append({'articleid': row['_id'], 'authorid':author_id, 'rank':  entry['rank']}, ignore_index=True)

    #print(df_authors)   
    
    #print(df_article_authors)
    
    return df_authors, df_article_authors