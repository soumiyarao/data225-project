import argparse
import mysql.connector
from mysql.connector import Error
import numpy as np

def get_db_credentials():
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description='Process dataset and create database')
    
    # Add command-line arguments
    parser.add_argument('host', type=str, help='host address of databse')
    parser.add_argument('user', type=str, help='database user')
    parser.add_argument('password', type=str, help='database password')

    try:
        # Parse the command-line arguments
        args = parser.parse_args()
    except argparse.ArgumentError:
        print('Error: Required arguments not provided')
        quit()
    
    return { 'host': args.host, 'user': args.user, 'password': args.password }

def get_db_connection(host, user, password, database=None):
    try:    
        if (database == None):
            connection = mysql.connector.connect(host=host, user=user, password=password)
        else:
            connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        print(f"Sucessfully connected to MySQL as {user} user.")
    except Error as e:
        print(f"Error while connecting to MySQL as {user} user.", e)
        quit()
    return connection

def insert_data(cursor, df, headers, table_name, insert_table_query):
     # Insert data into table
    for _, row in df.iterrows():
        data = []
        for header in headers:
            if type(row[header]) is np.int64:  
                data.append(int(row[header]))
            else:
                data.append(row[header])
        try:
            cursor.execute(insert_table_query,  data)
        except Exception as e:
            pass
            print("Exception:", e)          
    
    print(f"Dataframe inserted into table - {table_name}.")
      
def create_database(host, user, password, database):
    try:
        # Connect to MySQL server
        connection = get_db_connection(host, user, password)
        if connection.is_connected():
            cursor = connection.cursor()
            
            #drop_db_query = f"DROP DATABASE IF EXISTS {database}"
            #cursor.execute(drop_db_query)
            #print(f"Dropped {database} successfully.")

            # Create database if it does not exist
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database}"
            cursor.execute(create_db_query)
            print(f"Database {database} created successfully.")
            
            # Commit changes and close cursor
            connection.commit()
            cursor.close()

    except Error as e:
        print("Error while creating database in MySQL.", e)

    finally:
        if connection.is_connected():
            connection.close()
           
def create_insert_table(df, host, user, password, database, table_name, create_table_query, headers, insert_table_query):
    try:
        # Connect to MySQL server
        connection = get_db_connection(host, user, password, database)
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Drop table if it exists
            #drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
            #cursor.execute(drop_table_query)
            #print(f"Dropped table {table_name} if it exists.")

            # Create new table
            cursor.execute(create_table_query)
            print(f"Created new table {table_name}.")

            insert_data(cursor, df, headers, table_name, insert_table_query)
            
            # Commit changes and close cursor
            connection.commit()
            cursor.close()

    except Error as e:
        print(f"Error while creating {table_name} in {database}.", e)
        quit()
        
    finally:
        if connection.is_connected():
            connection.close()

def rename_table(host, user, password, database, old_table_name, new_table_name):
    try:
        connection = get_db_connection(host, user, password, database)
        if connection.is_connected():
            cursor = connection.cursor()
            sql = f"RENAME TABLE {old_table_name} TO {new_table_name}"
            cursor.execute(sql)
            print(f"Renamed table {old_table_name} to {new_table_name}.")
            connection.commit()
            cursor.close()
            connection.close()
    except Error as e:
        print("Error while renaming table in MySQL.", e)

    finally:
        if connection.is_connected():
            connection.close()