# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG
# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import timedelta, datetime
# Importing operators 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# Step 2: Initiating the default_args
# default_args = {
#         'owner' : 'airflow',
#         'start_date' : datetime(2022, 11, 12),
#
# }
def first_function_execute():
    print("Hello world")
    return "Hello world"

# Step 3: Creating DAG Object
dag = DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_daily":timedelta(minutes=5),
        "start_date":datetime(2025,1,1)
    },
    catchup = False)


first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        dag=dag
    )
