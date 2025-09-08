from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime 

with DAG(
    dag_id ="hello_airflow", 
    start_date = datetime(2025,9,8), 
    schedule_interval="@daily",
    catchup = False, 
    tags=["test"], 
) as dag:
    
    hello_task = BashOperator(
        task_id = "say_hello", 
        bash_command = "echo 'Hello, Airflow is working!'"
    )