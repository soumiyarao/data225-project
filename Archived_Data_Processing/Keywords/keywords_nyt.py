import pandas as pd
import ast
from ast import literal_eval
import mysql.connector
import json

df = pd.read_csv("PATH_CONFIGURE")

df['keywords'] = df['keywords'].fillna('[]')
#df = df[:3]

keywords = pd.DataFrame(columns=['id','name','value','rank','major'])

# Create empty lists to store the values
name_list = []
value_list = []
rank_list = []
major_list = []
id_list = []

for index, row in df.iterrows():
#for row in df['keywords']:
    article_id = row['id']
    string_value = row['keywords']
    #print (string_value)
    json_string = string_value.replace("{\'name\': \'", "{\"name\": \"")
    #print (json_string)
    json_string = json_string.replace("\', \'value\': \"", "\", \"value\": \"")
    #print (json_string)
    json_string = json_string.replace("\', \'value\': \'", "\", \"value\": \"")
    #print (json_string)
    json_string = json_string.replace("\", \'rank\'", "\", \"rank\"")
    #print (json_string)
    json_string = json_string.replace("\', \'rank\'", "\", \"rank\"")
    #print (json_string)
    json_string = json_string.replace(", \'major\': \'N\'", ", \"major\": \"N\"")
   # print (json_string)
    json_string = json_string.replace(", \'major\': \'Y\'", ", \"major\": \"Y\"")
   # print (json_string)
    json_string = json_string.replace("\"FREE TRADE,\"", "\'FREE TRADE,\'")
    #print (json_string)
    if json_string == " Ph.D.":
        continue
    #print ("\nparsings done\n")
    # Load the JSON string as a Python object (dictionary)
    json_object = json.loads(json_string)
    # Print the JSON object
    #print(json_object)
    #print ("\njson printed\n")
    for dict_entry in json_object:
        #print (dict_entry)
        if 'name' in dict_entry:
            name_list.append(dict_entry['name'])
        if 'value' in dict_entry:
            value_list.append(dict_entry['value'])
        if 'rank' in dict_entry:
            rank_list.append(dict_entry['rank'])
        if 'major' in dict_entry:
            major_list.append(dict_entry['major'])
        id_list.append(article_id)
print ("\n\ndone processing\n\n")
#print (name_list)
#print (value_list)
#print (rank_list)
#print (major_list)
#print (id_list)

keywords['id'] = id_list
keywords['name'] = name_list
keywords['name'] = keywords['name'].astype(str)
keywords['value'] = value_list
keywords['value'] = keywords['value'].astype(str)
keywords['rank'] = rank_list
keywords['major'] = major_list
keywords['major'] = keywords['major'].astype(str)

print(keywords.dtypes)
print(keywords.head())


connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='PASSWORD_CONFIGURE',
    database='nyt'
)

print("Sucessfully connected to MySQL")

if connection.is_connected():
    cursor = connection.cursor()
    
    # Drop table if it exists
    drop_table_query = "DROP TABLE IF EXISTS keywords_nyt"
    cursor.execute(drop_table_query)
    print("Dropped table")

    cursor.execute("CREATE TABLE keywords_nyt (keyword_id INT AUTO_INCREMENT PRIMARY KEY,keyword_name VARCHAR(300),keyword_value  VARCHAR(300),keyword_rank INT, major  VARCHAR(100),article_id VARCHAR(300) );")
    print("Created table")

    # Remove duplicates
    #keywords.drop_duplicates(subset=['id'], keep='first', inplace=True)
    #keywords.drop_duplicates(subset=['name'], keep='first', inplace=True)

    insert_query=("INSERT INTO keywords_nyt(keyword_name,keyword_value,keyword_rank,major,article_id) VALUES (%s, %s, %s, %s, %s)")
    for index, row in keywords.iterrows():
        cursor.execute(insert_query, (row['name'],row['value'],row['rank'],row['major'],row['id']))
    print("Inserted Data")

    # Commit changes and close cursor
    connection.commit()
    cursor.close()

if connection.is_connected():
    connection.close()
