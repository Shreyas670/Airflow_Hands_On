from airflow.sdk import asset


# Creating first asset with schedule to run daily. It returns value which will be used in downstream assets.
@asset(schedule="@daily")
def extracted_data():
    return {"a": 1, "b": 2}


# Another asset which is linked to exracted_data asset. So whenever DAG of extracted_data completes, this asset's DAG run will be trigerred automatically.
@asset(schedule=extracted_data)
def transformed_data(context):
  # As we need data of other DAG run, we need to use xcom_pull and give the details of which DAG and in that DAG which task's data needs to be pulled here.
    data = context["ti"].xcom_pull(
        dag_id="extracted_data",
        task_ids="extracted_data",
        key="return_value",
        include_prior_dates=True,
    )
    return {k: v * 2 for k, v in data.items()}

# Similarly another asset is created which is run after transformed_data.
@asset(schedule=transformed_data)
def loaded_data(context):
  # Data from transformed_data DAG is pulled in here.
    data = context["task_instance"].xcom_pull(
        dag_id="transformed_data",
        task_ids="transformed_data",
        key="return_value",
        include_prior_dates=True,
    )
    summed_data = sum(data.values())
    print(f"Summed data: {summed_data}")
