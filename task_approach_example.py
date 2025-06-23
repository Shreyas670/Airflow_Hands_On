from airflow.sdk import Asset, dag, task

# Here instead of directly using assets decorator, we create DAGs and Tasks and use assets inside them.
@dag(schedule="@daily")
def extract_dag():
  # Task is created with outlet as extracted_data asset, which means once this task is completed, it updates the extracted_data asset
    @task(outlets=[Asset("extracted_data")])
    def extract_task():
        return {"a": 1, "b": 2}

    extract_task()


extract_dag()

# Another DAG which runs when extracted_data asset is updated.
@dag(schedule=[Asset("extracted_data")])
def transform_dag():
    # Here this task updated transformed_data asset.
    @task(outlets=[Asset("transformed_data")])
    def transform_task(**context):
        data = context["ti"].xcom_pull(
            dag_id="extract_dag",
            task_ids="extract_task",
            key="return_value",
            include_prior_dates=True,
        )
        return {k: v * 2 for k, v in data.items()}

    transform_task()


transform_dag()


@dag(schedule=[Asset("transformed_data")])
def load_dag():
    # There is no outlet asset for this task.
    @task
    def load_task(**context):
        data = context["ti"].xcom_pull(
            dag_id="transform_dag",
            task_ids="transform_task",
            key="return_value",
            include_prior_dates=True,
        )
        summed_data = sum(data.values())
        print(f"Summed data: {summed_data}")

    load_task()


load_dag()
