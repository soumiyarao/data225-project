#Apache AirFlow Pipelines

The nyt_dag.py script defines 3 Airflow DAG pipelines:
- real_time_api_pipeline: A pipeline to fetch data from NYT Archive Data real time api, process the data and insert the data into snowflake database NYT_DB.NYT_SCHEMA. This pipeline is scheduled to run every 1st day of month at 12 am.
- transformation_pipeline: A pipeline to apply transformations and performs analytics. The summarized results are loaded to snowflake database NYT_DB.NYT_RESULTS_SCHEMA. This pipeline is scheduled to run every 1st day of month at 6 am.
- ml_classification_pipeline: A pipeline to run ML analysis using Snowflake ML Classifications Algorithm. This pipleine trains ML model to predict section name based on input features and the result metrics are stored in in NYT_DB.NYT_RESULTS_SCHEMA. This pipeline is scheduled to run every 1st day of month at 12 pm.

To run the piplines start the Airflow though terminal:
astro dev start

To view the pipeline go to Airflow Webserver: http://localhost:8080 on the browser.
