# Сбор данных

## Цель

В рамках данного ДЗ вы научитесь настраивать сбор данных по расписанию.

## Описание/Пошаговая инструкция выполнения домашнего задания

1. Напишите скрипт, который генерирует новую порцию данных и сохраняет его в HDFS.
2. Автоматизируйте регулярный запуск через AirFlow.

## Скрипт для регулярной проверки и загрузки данных из s3 в hdfs

Скрипт находится на мастер узле dataproc кластера.

[`/home/ubuntu/check_and_upload_new_data.sh`](scripts/check_and_upload_new_data.sh):
```
s3_file_list=$(s3cmd ls s3://mlops-data/fraud-data/ | awk '{print $4}' | awk -F" +|/" '{print $NF}' | sort)
hdfs_file_list=$(hdfs dfs -ls -C /user/fraud-data/ | awk -F" +|/" '{print $NF}' | sort)

# Get first missing file
file_to_add=$(comm -23 <(echo "$s3_file_list" | sort) <(echo "$hdfs_file_list" | sort) | head -n 1)

if [ -z "$file_to_add" ]
then
    echo "No files to download"
else
    echo "Downloading $file_to_add"
    s3cmd get s3://mlops-data/fraud-data/$file_to_add ./$file_to_add
    hdfs dfs -put ./$file_to_add /user/fraud-data/
    rm ./$file_to_add
fi
```

Скрипт сверяет список файлов на s3 и hdfs и загружает один новый файл если есть различия. Загружаю только один чтобы симулировать ежедневную подгрузку данных.

## DAG для запуска скрипта по расписанию

[`/home/airflow/dags/download_on_schedule.py`](dags/download_on_schedule.py):
```
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
```

## Лог выполнения DAG и результат в hdfs

```
*** Reading local file: /var/log/airflow/download_on_schedule/ssh_download_on_schedule/2023-05-08T19:34:56.205505+00:00/1.log
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1032} INFO - Dependencies all met for <TaskInstance: download_on_schedule.ssh_download_on_schedule manual__2023-05-08T19:34:56.205505+00:00 [queued]>
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1032} INFO - Dependencies all met for <TaskInstance: download_on_schedule.ssh_download_on_schedule manual__2023-05-08T19:34:56.205505+00:00 [queued]>
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1238} INFO - 
--------------------------------------------------------------------------------
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1239} INFO - Starting attempt 1 of 1
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1240} INFO - 
--------------------------------------------------------------------------------
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1259} INFO - Executing <Task(SSHOperator): ssh_download_on_schedule> on 2023-05-08 19:34:56.205505+00:00
[2023-05-08, 19:34:57 UTC] {standard_task_runner.py:52} INFO - Started process 526859 to run task
[2023-05-08, 19:34:57 UTC] {standard_task_runner.py:76} INFO - Running: ['***', 'tasks', 'run', 'download_on_schedule', 'ssh_download_on_schedule', 'manual__2023-05-08T19:34:56.205505+00:00', '--job-id', '123', '--raw', '--subdir', 'DAGS_FOLDER/download_on_schedule.py', '--cfg-path', '/tmp/tmpgnbowxk0', '--error-file', '/tmp/tmplb0umkkp']
[2023-05-08, 19:34:57 UTC] {standard_task_runner.py:77} INFO - Job 123: Subtask ssh_download_on_schedule
[2023-05-08, 19:34:57 UTC] {logging_mixin.py:109} INFO - Running <TaskInstance: download_on_schedule.ssh_download_on_schedule manual__2023-05-08T19:34:56.205505+00:00 [running]> on host ***-yandex.ru-central1.internal
[2023-05-08, 19:34:57 UTC] {taskinstance.py:1424} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=***
AIRFLOW_CTX_DAG_ID=download_on_schedule
AIRFLOW_CTX_TASK_ID=ssh_download_on_schedule
AIRFLOW_CTX_EXECUTION_DATE=2023-05-08T19:34:56.205505+00:00
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-05-08T19:34:56.205505+00:00
[2023-05-08, 19:34:57 UTC] {ssh.py:137} INFO - Creating ssh_client
[2023-05-08, 19:34:57 UTC] {ssh.py:115} INFO - ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook.
[2023-05-08, 19:34:57 UTC] {base.py:70} INFO - Using connection to: id: ssh_dataproc. Host: 10.128.0.11, Port: None, Schema: , Login: ubuntu, Password: None, extra: {'key_file': '/home/***/.ssh/id_rsa'}
[2023-05-08, 19:34:57 UTC] {ssh.py:291} WARNING - No Host Key Verification. This won't protect against Man-In-The-Middle attacks
[2023-05-08, 19:34:57 UTC] {transport.py:1873} INFO - Connected (version 2.0, client OpenSSH_8.2p1)
[2023-05-08, 19:34:57 UTC] {transport.py:1873} INFO - Authentication (publickey) successful!
[2023-05-08, 19:34:57 UTC] {ssh.py:469} INFO - Running command: source /home/ubuntu/.profile; /home/ubuntu/check_and_upload_new_data.sh 
[2023-05-08, 19:34:59 UTC] {ssh.py:501} INFO - Downloading 2020-02-18.txt
[2023-05-08, 19:35:00 UTC] {ssh.py:501} INFO - download: 's3://mlops-data/fraud-data/2020-02-18.txt' -> './2020-02-18.txt'  [1 of 1]
[2023-05-08, 19:35:00 UTC] {ssh.py:501} INFO - 
      65536 of 2995431240     0% in    0s   372.18 kB/s
[2023-05-08, 19:35:01 UTC] {ssh.py:501} INFO - 
   80805888 of 2995431240     2% in    1s    65.75 MB/s
[2023-05-08, 19:35:02 UTC] {ssh.py:501} INFO - 
  188809216 of 2995431240     6% in    2s    80.12 MB/s
[2023-05-08, 19:35:03 UTC] {ssh.py:501} INFO - 
  267452416 of 2995431240     8% in    3s    77.35 MB/s
[2023-05-08, 19:35:04 UTC] {ssh.py:501} INFO - 
  355532800 of 2995431240    11% in    4s    78.66 MB/s
[2023-05-08, 19:35:05 UTC] {ssh.py:501} INFO - 
  426835968 of 2995431240    14% in    5s    76.64 MB/s
[2023-05-08, 19:35:06 UTC] {ssh.py:501} INFO - 
  515309568 of 2995431240    17% in    6s    77.86 MB/s
[2023-05-08, 19:35:07 UTC] {ssh.py:501} INFO - 
  608632832 of 2995431240    20% in    7s    79.36 MB/s
[2023-05-08, 19:35:08 UTC] {ssh.py:501} INFO - 
  670105600 of 2995431240    22% in    8s    74.25 MB/s
[2023-05-08, 19:35:09 UTC] {ssh.py:501} INFO - 
  812187648 of 2995431240    27% in    9s    80.62 MB/s
[2023-05-08, 19:35:10 UTC] {ssh.py:501} INFO - 
  921763840 of 2995431240    30% in   10s    82.70 MB/s
[2023-05-08, 19:35:11 UTC] {ssh.py:501} INFO - 
  975241216 of 2995431240    32% in   11s    79.91 MB/s
[2023-05-08, 19:35:12 UTC] {ssh.py:501} INFO - 
 1069613056 of 2995431240    35% in   12s    79.90 MB/s
[2023-05-08, 19:35:14 UTC] {ssh.py:501} INFO - 
 1117716480 of 2995431240    37% in   14s    75.61 MB/s
[2023-05-08, 19:35:16 UTC] {ssh.py:501} INFO - 
 1189150720 of 2995431240    39% in   16s    70.83 MB/s
[2023-05-08, 19:35:17 UTC] {ssh.py:501} INFO - 
 1300496384 of 2995431240    43% in   17s    72.91 MB/s
[2023-05-08, 19:35:20 UTC] {ssh.py:501} INFO - 
 1373831168 of 2995431240    45% in   20s    65.35 MB/s
[2023-05-08, 19:35:21 UTC] {ssh.py:501} INFO - 
 1438318592 of 2995431240    48% in   21s    65.15 MB/s
[2023-05-08, 19:35:22 UTC] {ssh.py:501} INFO - 
 1518862336 of 2995431240    50% in   22s    65.65 MB/s
[2023-05-08, 19:35:24 UTC] {ssh.py:501} INFO - 
 1580204032 of 2995431240    52% in   24s    61.10 MB/s
[2023-05-08, 19:35:25 UTC] {ssh.py:501} INFO - 
 1660026880 of 2995431240    55% in   25s    61.69 MB/s
[2023-05-08, 19:35:26 UTC] {ssh.py:501} INFO - 
 1704132608 of 2995431240    56% in   26s    60.93 MB/s
[2023-05-08, 19:35:29 UTC] {ssh.py:501} INFO - 
 1743650816 of 2995431240    58% in   29s    57.17 MB/s
[2023-05-08, 19:35:30 UTC] {ssh.py:501} INFO - 
 1807351808 of 2995431240    60% in   30s    57.27 MB/s
[2023-05-08, 19:35:31 UTC] {ssh.py:501} INFO - 
 1847066624 of 2995431240    61% in   31s    56.63 MB/s
[2023-05-08, 19:35:33 UTC] {ssh.py:501} INFO - 
 1854668800 of 2995431240    61% in   33s    53.09 MB/s
[2023-05-08, 19:35:34 UTC] {ssh.py:501} INFO - 
 1932197888 of 2995431240    64% in   34s    53.69 MB/s
[2023-05-08, 19:35:35 UTC] {ssh.py:501} INFO - 
 1932263424 of 2995431240    64% in   35s    51.52 MB/s
[2023-05-08, 19:35:36 UTC] {ssh.py:501} INFO - 
 2008547328 of 2995431240    67% in   36s    52.08 MB/s
[2023-05-08, 19:35:37 UTC] {ssh.py:501} INFO - 
 2028797952 of 2995431240    67% in   37s    51.21 MB/s
[2023-05-08, 19:35:38 UTC] {ssh.py:501} INFO - 
 2102591488 of 2995431240    70% in   38s    51.71 MB/s
[2023-05-08, 19:35:40 UTC] {ssh.py:501} INFO - 
 2133458944 of 2995431240    71% in   40s    50.04 MB/s
[2023-05-08, 19:35:42 UTC] {ssh.py:501} INFO - 
 2134310912 of 2995431240    71% in   42s    48.45 MB/s
[2023-05-08, 19:35:43 UTC] {ssh.py:501} INFO - 
 2202009600 of 2995431240    73% in   43s    48.82 MB/s
[2023-05-08, 19:35:44 UTC] {ssh.py:501} INFO - 
 2240151552 of 2995431240    74% in   44s    48.53 MB/s
[2023-05-08, 19:35:45 UTC] {ssh.py:501} INFO - 
 2278293504 of 2995431240    76% in   45s    48.25 MB/s
[2023-05-08, 19:35:46 UTC] {ssh.py:501} INFO - 
 2298347520 of 2995431240    76% in   46s    47.26 MB/s
[2023-05-08, 19:35:50 UTC] {ssh.py:501} INFO - 
 2327904256 of 2995431240    77% in   50s    44.00 MB/s
[2023-05-08, 19:35:53 UTC] {ssh.py:501} INFO - 
 2337341440 of 2995431240    78% in   53s    41.93 MB/s
[2023-05-08, 19:35:55 UTC] {ssh.py:501} INFO - 
 2400256000 of 2995431240    80% in   55s    41.56 MB/s
[2023-05-08, 19:35:56 UTC] {ssh.py:501} INFO - 
 2507210752 of 2995431240    83% in   56s    42.63 MB/s
[2023-05-08, 19:35:57 UTC] {ssh.py:501} INFO - 
 2583298048 of 2995431240    86% in   57s    43.16 MB/s
[2023-05-08, 19:35:59 UTC] {ssh.py:501} INFO - 
 2595422208 of 2995431240    86% in   59s    41.67 MB/s
[2023-05-08, 19:36:00 UTC] {ssh.py:501} INFO - 
 2671771648 of 2995431240    89% in   60s    42.19 MB/s
[2023-05-08, 19:36:01 UTC] {ssh.py:501} INFO - 
 2747596800 of 2995431240    91% in   61s    42.68 MB/s
[2023-05-08, 19:36:03 UTC] {ssh.py:501} INFO - 
 2771517440 of 2995431240    92% in   63s    41.84 MB/s
[2023-05-08, 19:36:04 UTC] {ssh.py:501} INFO - 
 2805399552 of 2995431240    93% in   64s    41.68 MB/s
[2023-05-08, 19:36:05 UTC] {ssh.py:501} INFO - 
 2878799872 of 2995431240    96% in   65s    42.12 MB/s
[2023-05-08, 19:36:07 UTC] {ssh.py:501} INFO - 
 2907308032 of 2995431240    97% in   67s    41.07 MB/s
[2023-05-08, 19:36:09 UTC] {ssh.py:501} INFO - 
 2919301120 of 2995431240    97% in   69s    40.17 MB/s
[2023-05-08, 19:36:11 UTC] {ssh.py:501} INFO - 
 2991652864 of 2995431240    99% in   71s    39.70 MB/s
[2023-05-08, 19:36:12 UTC] {ssh.py:501} INFO - 
 2995431240 of 2995431240   100% in   72s    39.59 MB/s  done
[2023-05-08, 19:36:33 UTC] {taskinstance.py:1267} INFO - Marking task as SUCCESS. dag_id=download_on_schedule, task_id=ssh_download_on_schedule, execution_date=20230508T193456, start_date=20230508T193457, end_date=20230508T193633
[2023-05-08, 19:36:33 UTC] {local_task_job.py:154} INFO - Task exited with return code 0
[2023-05-08, 19:36:33 UTC] {local_task_job.py:264} INFO - 0 downstream tasks scheduled from follow-on schedule check
```

Результат на hdfs в dataproc (появился файл 2020-02-18.txt):
 ![Dataproc listing](img/3_hdfs_result.png "Dataproc listing")