from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'my_airflow_dag',
    default_args=default_args,
    description='A simple Airflow DAG with multiple tasks',
    schedule_interval=timedelta(days=1),  # Set the interval at which the DAG should run
)

# Define three Python functions to be used as tasks

def task_1_function(**kwargs):
    print("Executing Task 1")
    # Your task 1 logic here

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1_function,
    provide_context=True,
    dag=dag,
)

def task_2_function(**kwargs):
    print("Executing Task 2")
    # Your task 2 logic here

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2_function,
    provide_context=True,
    dag=dag,
)

def task_3_function(**kwargs):
    print("Executing Task 3")
    # Your task 3 logic here

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3_function,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_1 >> task_2 >> task_3

if __name__ == "__main__":
    dag.cli()
