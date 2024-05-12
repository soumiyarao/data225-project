from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from datetime import datetime, timedelta
import os
import process_data

snowflake_ml_classification_query = '''
    use role ACCOUNTADMIN;
    use warehouse NYT_WH;
    use database NYT_DB;
    use schema NYT_RESULTS_SCHEMA;

    CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION trail(
        INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'TEXT_CLASSIFICATION_INPUT'),
        TARGET_COLNAME => 'SECTION_NAME',
        CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
    );

    CREATE TABLE classifications AS SELECT
        *, 
        trail!PREDICT(
            OBJECT_CONSTRUCT(*),
            -- This option alows the prediction process to complete even if individual rows must be skipped.
            {'ON_ERROR': 'SKIP'}
        ) as predictions
    from TEXT_CLASSIFICATION_INPUT;

    -- Parse the prediction results into separate columns. 
    -- Note: This is a just an example. Be sure to update this to reflect 
    -- the classes in your dataset.
    DROP TABLE IF EXISTS CLASSIFICATIONS_RESULTS;
    CREATE TABLE CLASSIFICATIONS_RESULTS AS 
    SELECT 
        *,
        REPLACE(predictions:"class"::string, '"', '') AS predicted_section_name,
        ROUND(predictions['probability'][REPLACE(predictions:"class"::string, '"', '')], 3) AS probability
    FROM 
        classifications;

    DROP TABLE CLASSIFICATIONS;
    DROP TABLE IF EXISTS classifications_metrics;
    CREATE TABLE classifications_metrics AS 
    SELECT 
        SECTION_NAME AS group_name,
        COUNT(*) AS actual_count,
        SUM(CASE WHEN SECTION_NAME = PREDICTED_SECTION_NAME THEN 1 ELSE 0 END) AS correct_predictions,
        COUNT(*) - SUM(CASE WHEN SECTION_NAME = PREDICTED_SECTION_NAME THEN 1 ELSE 0 END) AS incorrect_predictions,
        ROUND((correct_predictions/actual_count)*100, 2) AS accuracy
    FROM 
        classifications_results
    GROUP BY 
        SECTION_NAME
    ORDER BY
        correct_predictions DESC;'''
    
real_time_api_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12, 0 , 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ml_pipeline_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12, 12, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn_new", 
        profile_args={"database": "nyt_db", "schema": "nyt_results_schema"},
    )
)
            
dag1= DAG(
    dag_id = 'real_time_api_pipeline',
    default_args=real_time_api_args,
    description='Fetch data from New York Times API and load into Snowflake',
    schedule_interval="@monthly",  
    catchup=False,
    
) 

with dag1:
    
    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable= process_data.preprocessing,
    )
    load_data = PythonOperator(
        task_id='load_data',
        python_callable= process_data.insert_data,
        op_args=preprocess_task.output,
    )
    
    preprocess_task >> load_data
    
      
dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/nyt_results_pipeline",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval='0 6 1 * *',
    start_date=datetime(2024, 5, 12, 6, 0),
    catchup=False,
    dag_id = 'transformation_pipeline',
)


dag2 = DAG(
    dag_id = 'ml_classification_pipeline',
    default_args=ml_pipeline_args,
    description='Fetch data from New York Times API and load into Snowflake',
    schedule_interval='0 12 1 * *',  
    catchup=False,
    
) 

with dag2:
        
    snowflake_task = SnowflakeOperator(
        task_id='snoflake_ml_classification_task',
        sql=snowflake_ml_classification_query,
        snowflake_conn_id="snowflake_conn_new",
        autocommit=True,
    )
  
    snowflake_task