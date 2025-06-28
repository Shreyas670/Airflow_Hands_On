from airflow.sdk import dag, task, Context

# We can use xcom push and pull methods to share data between two tasks
@dag
def xcom_dag():

    @task
    def t1(context: Context): # we need the context of the task instance to perform xcom
        val = 42
        context['ti'].xcom_push(key='my_key',value=val) # xcom push will store the value in metadata db with details such as key, value, task, dag, etc.

    @task
    def t2(context: Context):
        val = context['ti'].xcom_pull(task_ids='t1', key='my_key') # xcom pull will fetch the value based on the parameters given.
        print(val)

    t1() >> t2()

xcom_dag()

# We saw using xcom push and pull how we can share data between different tasks. However this method is an overkill and requires lot of code to be written
#Another way of sharing data is just by returning the data by sender task and using that data as parameter in receiver task.

@dag
def xcom_dag1():

    @task
    def t1() -> int:
        val = 42
        return val # We can just return the data from this task. This on the backend will perform the xcom push

    @task
    def t2(val: int): # we can define that this task is expecting data
        print(val)

    # Here we can send the output returned by task1 as input to task2 thus exchanging data
    val = t1() 
    t2(val)
 
xcom_dag1()
