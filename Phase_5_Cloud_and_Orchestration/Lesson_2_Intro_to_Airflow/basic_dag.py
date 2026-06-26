# basic_dag.py
# Beginner Level: Your first Airflow workflow.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Define the "Logic" (What we want to do)
def hello_data_engineer():
    print("Welcome to the world of Orchestration!")

def run_fake_ingestion():
    print("--- [SIMULATING] Data Ingestion ---")

# 2. Define the "DAG" (When and How to run it)
with DAG(
    dag_id='my_first_pipeline',
    start_date=datetime(2024, 3, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 3. Define the "Tasks"
    task_welcome = PythonOperator(
        task_id='welcome_task',
        python_callable=hello_data_engineer
    )

    task_ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=run_fake_ingestion
    )

    # 4. Set the Order (Dependencies)
    task_welcome >> task_ingest

# 🏛️ Architect's Tip:
# "Always keep your DAGs simple. If a DAG has 100 tasks, it's 
# too complex to debug. Break it into smaller, manageable DAGs."
