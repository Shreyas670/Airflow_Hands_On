from airflow.sdk import dag
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
import os


@dag
def postgres_to_neo4j(): 
    # SQLExecuteQueryOperator is used which is used to execute a sql query in any rdbms.
    # This sql query runs in postgres which is running in local, hence it is able to access the local location. If this query ran inside airflow container, then it wont be able to access the local folder.
    export_data = SQLExecuteQueryOperator (
        task_id="export_data",
        conn_id="postgres_local",
        sql="""
        COPY students TO 'C:/Users/Public/Downloads/students.csv' WITH ( 
        FORMAT CSV,
        HEADER,         -- Includes the column names as the first row in the CSV
        DELIMITER ',',  -- Specifies comma as the column separator
        ENCODING 'UTF8' -- Specifies the character encoding
        )    
        """
    )
    # Neo4jOperator is used to run the cipher query on neo4j db. Neo4j provider was installed to use this.
    # We are importing data from import folder of neo4j which is mounted to a local folder where data was exported from postgres
    import_data = Neo4jOperator(
        task_id = "import_data",
        sql="""LOAD CSV WITH HEADERS FROM 'file:///students.csv' AS row
                MERGE (s:Student {id: toInteger(row.id)})
                ON CREATE SET
                    s.name = row.name,
                    s.class = row.class,
                    s.age = toInteger(row.age),
                    s.gender = row.gender
                ON MATCH SET 
                    s.name = row.name,
                    s.class = row.class,
                    s.age = toInteger(row.age),
                    s.gender = row.gender""",
        neo4j_conn_id="neo4j",
        parameters="None",
    )

    export_data >> import_data
    
postgres_to_neo4j()
