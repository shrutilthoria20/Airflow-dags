from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import csv

def first_function_execute():
    with open('file.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'age'])
        writer.writerow(['John Doe', 30])


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
