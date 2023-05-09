from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='download_on_schedule',
    default_args=args,
    schedule_interval='0 21 * * *',
    start_date=datetime.now(),
    tags=['API'],
    catchup=False
)

ssh_download_on_schedule = SSHOperator(
    task_id="ssh_download_on_schedule",
    ssh_conn_id='ssh_dataproc',
    command='source /home/ubuntu/.profile; /home/ubuntu/check_and_upload_new_data.sh ',
    dag=dag
)

ssh_download_on_schedule