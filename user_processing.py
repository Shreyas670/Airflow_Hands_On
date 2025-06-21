from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Dag is created using Dag decorator
@dag
def user_processing():

    #First task : To create target table in postgres database where data will be loaded. postgres connection is created in airflow UI.
    create_table = SQLExecuteQueryOperator (
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )            
        """
    )

    # Second Task: This is a sensor operator created using task decorator. PokeReturnValue provides a granular way of working with sensors, where along with True/False info of whether poke worked or not, we can also return data using it
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue: 
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    

    # Third Task created using task decorator: Used to convert json into dictionary
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    # Fourth Task created using task decorator: Used to write contents of dictionary into CSV   
    @task
    def process_user(user_info):
        import csv, datetime

        user_info["created_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames= user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
    
    # Fourth Task created using task decorator: Uses PostgresHook to load data from csv to table 
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql= "COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )

    # Creating dependencies within the tasks. As output of some functions are used by the next one, instead of just using >>, we can use below syntax
    process_user(extract_user(create_table >> is_api_available())) >> store_user()

    # fake_user = is_api_available()
    # user_info = extract_user(fake_user)
    # process_user(user_info)
    # store_user()

user_processing()