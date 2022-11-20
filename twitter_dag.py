from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
from twitter_etl import run_twitter_etl


default_dag_args = {
    'owner' :  'Abs',
    'depends_on_past': False,
    'start_date': datetime(2022,11,19),
    'email':['benigmim.ayoub@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'project_id': models.Variable.get('gcp_project')
}

dag = DAG(
    'twitter_dag',
    default_args=default_dag_args,
    description='My first Apache airflow ETL'
)


run_etl = PythonOperator(
    task_id = 'scraping morrocan gov tweets etl',
    python_callable= run_twitter_etl,
    dag = dag
)


run_etl