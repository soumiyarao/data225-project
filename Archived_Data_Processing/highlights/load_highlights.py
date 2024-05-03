import pandas as pd
import mysql.connector
import ast

df = pd.read_csv("/Users/Downloads/xxxx.csv") #Configure
hdf = df[['_id','headline']]

list_of_dicts = []


for index, row in hdf.iterrows():
    string_value = row['headline']
    json_string = string_value.replace(': None', ": \'None\'")
    json_string = json_string.replace("{\'main\': \'", "{\"main\": \"")
    json_string = json_string.replace("{\'main\': \"", "{\"main\": \"")

    json_string = json_string.replace("\', \'kicker\': \'", "\", \"kicker\": \"")
    json_string = json_string.replace("\", \'kicker\': \'", "\", \"kicker\": \"")
    json_string = json_string.replace("\", \'kicker\': \"", "\", \"kicker\": \"")
    json_string = json_string.replace("\', \'kicker\': \"", "\", \"kicker\": \"")

    json_string = json_string.replace("\', \'content_kicker\': \'", "\", \"content_kicker\": \"")
    json_string = json_string.replace("\", \'content_kicker\': \'", "\", \"content_kicker\": \"")
    json_string = json_string.replace("\", \'content_kicker\': \"", "\", \"content_kicker\": \"")
    json_string = json_string.replace("\', \'content_kicker\': \"", "\", \"content_kicker\": \"")

    json_string = json_string.replace("\', \'print_headline\': \'", "\", \"print_headline\": \"")
    json_string = json_string.replace("\", \'print_headline\': \'", "\", \"print_headline\": \"")
    json_string = json_string.replace("\", \'print_headline\': \"", "\", \"print_headline\": \"")
    json_string = json_string.replace("\', \'print_headline\': \"", "\", \"print_headline\": \"")

    json_string = json_string.replace("\', \'name\': \'", "\", \"name\": \"")
    json_string = json_string.replace("\", \'name\': \'", "\", \"name\": \"")
    json_string = json_string.replace("\", \'name\': \"", "\", \"name\": \"")
    json_string = json_string.replace("\', \'name\': \"", "\", \"name\": \"")

    json_string = json_string.replace("\', \'seo\': \'", "\", \"seo\": \"")
    json_string = json_string.replace("\', \'sub\'", "\", \"sub\"")
    json_string = json_string.replace("\": \'None\'}", "\": \"None\"}")
    parsed_dict = ast.literal_eval(json_string)
    parsed_dict['article_id'] = row['_id']
    list_of_dicts.append(parsed_dict)

hdf = pd.DataFrame(list_of_dicts)

connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='', #Configure
    database='nyt1'
)

print("Sucessfully connected to MySQL")

if connection.is_connected():
    cursor = connection.cursor()

    # Drop table if it exists
    drop_table_query = "DROP TABLE IF EXISTS highlights"
    cursor.execute(drop_table_query)
    print("Dropped table")

    cursor.execute("CREATE TABLE highlights ( highlight_id INT AUTO_INCREMENT PRIMARY KEY, main TEXT, kicker VARCHAR(255), content_kicker VARCHAR(255),   print_headline TEXT,  name VARCHAR(255),   seo VARCHAR(255), sub VARCHAR(255), article_id VARCHAR(255), FOREIGN KEY (article_id) REFERENCES article(_id));")
    print("Created table")

    cursor = connection.cursor()

    for row in list_of_dicts:
        sql = "INSERT INTO highlights (main, kicker, content_kicker, print_headline, name, seo, sub, article_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        values = (
        row["main"], row["kicker"], row["content_kicker"], row["print_headline"], row["name"], row["seo"], row["sub"], row["article_id"])
        cursor.execute(sql, values)

connection.commit()

cursor.close()
connection.close()





