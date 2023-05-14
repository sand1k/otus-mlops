from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='preprocess_data',
    default_args=args,
    schedule_interval='0 22 * * *',
    start_date=datetime.now(),
    tags=['API'],
    catchup=False
)

spark_submit = SparkSubmitOperator(
    task_id='spark_preprocess_data', 
    application ='/home/ubuntu/scripts/preprocess.py',
    name='fraud_data_preproc',
    dag=dag,
)

spark_submit