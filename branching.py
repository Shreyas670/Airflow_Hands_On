from airflow.sdk import dag, task

@dag
def branch():

    @task
    def a():
        return 1

    # This is a branch task whose job is to check a condition and determine which task to run next.
    @task.branch
    def b(val: int):
        if val == 1:
            return "equal_1"  # task that needs to be run next should be returned
        return "diff_than_1"

    @task
    def equal_1():
        print("equal_1")

    @task
    def diff_than_1():
        print("diff_than_1")

    val = a()
    b(val) >> [equal_1(),diff_than_1()]   # The different tasks that will be run based on the conditions should be given as a list

branch()
