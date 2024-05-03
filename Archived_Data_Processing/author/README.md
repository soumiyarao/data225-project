# data225-project

## Source code for preprocessing author data from nyt.csv file and loading into MySQL nyt databse

Instructions to execute the code:
- Copy the dataset nyt.csv file to DATA225-PROJECT/Archived_Data_Processing/author/res/ folder
- Launch MySQL
- Run the main program:
```
    python Archived_Data_Processing/author/src/main.py host username password
```
    - use localhost for running on local machine
    - username and password for MySQL workbench

NOTE: If running on windows:
In data_processing.py get_dataset() function uncomment:
```
file = f'{constants.DATA_SET_PATH}\\{file_name}.{constants.DATA_SET_EXTENSION}'