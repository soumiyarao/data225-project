import constants
import db_manager
import data_processing

# main function
def main():
    
    db_credentilas = db_manager.get_db_credentials() 
    host=db_credentilas['host']
    user=db_credentilas['user']
    password=db_credentilas['password']

    db_manager.create_database(host, user,password, constants.DATABASE_NAME)
    
    #LOAD CSV DATASET
    data_processing.load_articles(host, user, password)

    
# call main function
if __name__=="__main__": 
	main()
 