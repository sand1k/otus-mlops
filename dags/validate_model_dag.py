from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='validate_model',
    default_args=args,
    schedule_interval='0 23 * * *',
    start_date=datetime.now(),
    tags=['API'],
    catchup=False
)

spark_submit = SSHOperator(
    task_id="spark_validate_model-airflow",
    ssh_conn_id='ssh_dataproc',
    command='bash --login -c "spark-submit validate.py" ',
    dag=dag
)

spark_submit