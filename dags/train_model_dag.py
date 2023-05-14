from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='train_model',
    default_args=args,
    schedule_interval='0 23 * * *',
    start_date=datetime.now(),
    tags=['API'],
    catchup=False
)

spark_submit = SSHOperator(
    task_id="spark_train_model",
    ssh_conn_id='ssh_dataproc',
    command='bash --login -c "spark-submit train.py" ',
    dag=dag
)

spark_submit

# from datetime import datetime

# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

# args = {
#     'owner': 'ubuntu',
# }

# dag = DAG(
#     dag_id='train_model',
#     default_args=args,
#     schedule_interval='0 23 * * *',
#     start_date=datetime.now(),
#     tags=['API'],
#     catchup=False
# )

# spark_submit = SparkSubmitOperator(
#     task_id='spark_train_model', 
#     application ='/home/ubuntu/scripts/train.py' ,
#     name='fraud_data_train',
#     #jars='/home/airflow/mlflow-spark-1.24.0.jar',
#     dag=dag
# )

# spark_submit