# Регулярное переобучение

## Цель

В этом ДЗ вам нужно использовать инструменты для обучения моделей на больших данных и их версионирования.

## Описание/Пошаговая инструкция выполнения домашнего задания

1. Обучите модель с помощью pyspark для задачи fraud detection.
2. Настройте MLFlow для хранения артефактов модели (Object storage в качестве backend).
3. Сохраните артефакты с моделью в MLFLow.
4. Настройте в AirFlow регулярное переобучение модели на новых данных.

## Настройка

На Dataproc мастер узле:
```
sudo /opt/conda/bin/conda install -c conda-forge mlflow-skinny
```

Добавить в /etc/environment:
```
AWS_ACCESS_KEY_ID="<access key id>"
AWS_SECRET_ACCESS_KEY="<access key>"
MLFLOW_S3_ENDPOINT_URL="https://storage.yandexcloud.net"
MLFLOW_TRACKING_URI="http://10.128.0.39:5000"
AWS_DEFAULT_REGION="ru-central1"
```

Развернуть отдельную виртуальную машину для mlflow. На ней развернуть [mlflow-сревер](https://github.com/Toumash/mlflow-docker). Вместо minio настроить доступ к s3 на yandex cloud.

Запуск mlflow на виртуальной машине:
```
cd mlflow-docker
docker compose up -d
```
Сервер доступен на порту 5000.


## Скрипт обучения модели

[train.py](scripts/train.py)

Модель и метрики сохраняются в mlflow.

## DAG запуска скрипта обучения модели

[train_model_dag.py](dags/train_model_dag.py)

## Результаты выполнения DAG airflow

![MLFlow result](img/5_mlflow_result.png "Mlflow result")
![Airflow result](img/5_airflow_result.png "Airflow result")