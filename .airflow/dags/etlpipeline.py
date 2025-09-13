from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

## Define the DAG

with DAG(
    dag_id = "nasa_apod_postgress",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    ## Step 1: Create the postgress table if not exists

    @task
    def create_postgrees_table():
        ## Initialize the PostgreesHook
        postgree_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        ## Execute the table creation query
        postgree_hook.run(create_table_query)



    ## Step 2: Extract the nasa API Data(APOD), astronomy picture of the day is APOD
    ## https://api.nasa.gov/planetary/apod?api_key=IglRfMLTfpclI6jXDE5oydujihUuNZWuTF07oeVM
    extract_apod = SimpleHttpOperator(
        task_id = "extract_data_apod",
        http_conn_id = "nasa_api", ## Connection ID Defined In Airflow for NASA api
        endpoint = "planetary/apod", ## NASA API endpoin for APOD
        method="GET",
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection, here also we can hardocde the api_key
        response_filter = lambda response:response.json(), ## Convert response to json
    )


    ## Step 3: Transform the data (Pich the information that need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            "title":response.get("title", ""),
            "explanation":response.get("explanation", ""),
            "url":response.get("url", ""),
            "date":response.get("date", ""),
            "media_type":response.get("media_type", "")
        }

        return apod_data

    ## Step 4: Loading the data into the Postgress SQl
    @task
    def load_data_to_postgresql(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
        INSERT INTO apod_data(title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

        # Immediately fetch inserted data
        rows = postgres_hook.get_records("SELECT * FROM apod_data ORDER BY id DESC LIMIT 5;")
        print(f"Rows in table after insert: {rows}")

        print(f"Inserting row with date: {apod_data['date']} (type: {type(apod_data['date'])})")



    ## Step 5: Verify the data DBViewer


    ## Step 6 Define the task dependencies
    #Extract
    create_postgrees_table() >> extract_apod  ## Ensure the table is created before extraction
    api_response = extract_apod.output
    ## Transform
    transformed_data = transform_apod_data(api_response)
    ## Load
    load_data_to_postgresql(transformed_data)
    
