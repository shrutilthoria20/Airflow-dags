from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2
import json
import snowflake.connector
import os
import numpy as np
import mysql.connector


default_args={        "owner":"airflow",
        "retries":1,
        "retry_daily":timedelta(minutes=5),
        "start_date":datetime(2025,1,1)
    },

# Instantiate a DAG
dag = DAG(
    dag_id='data_dag',
    default_args=default_args,
    description='A simple Airflow DAG with multiple tasks',
    schedule_interval=timedelta(days=1),  # Set the interval at which the DAG should run
)

# Define three Python functions to be used as tasks

def task_1_function(**kwargs):
    myconn = mysql.connector.connect(host="localhost", user="root", password="", database="test1")
    mycursor=myconn.cursor()
    print("Database connected")
    query="SELECT * FROM data"
    print("Executing query")
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    print("Data fetched and sending to test2_connection")
    return myresult
    # Your task 1 logic here

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1_function,
    provide_context=True,
    dag=dag,
)

def task_2_function(**kwargs):
    print("Initializing database-2 connection")
    myconn = mysql.connector.connect(host="localhost", user="root", password="", database="test2")
    mycursor=myconn.cursor()
    print("Database connected")
    x=task_1_function()
    print("Data received from test1_connection")
    print("Inserting data")
    for i in x:
        sql = "insert into data(id,fname,lname) values (%s,%s,%s)"
        mycursor.execute(sql, i)
        myconn.commit()
    print("Data inserted")



task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2_function,
    provide_context=True,
    dag=dag,
)


# Set task dependencies
task_1 >> task_2

if __name__ == "__main__":
    dag.cli()
