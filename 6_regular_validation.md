# Валидация модели

Для валидации используются 2 модели:
- Staging модель из mlflow (текущая используемая модель)
- Latest модель из mlflow (последняя обученная модель)

Для каждой модели вычисляются метрики AUC и Accuracy (для fraud транзакций) на самых свежих данных и логгируются в mlflow. Если метрики свежей модели окажутся выше текущей, ее можно переместить в staging в интерфейсе mlflow.

Скрипт валидации (находится на master узле spark кластера): [validate.py](scripts/validate.py)

DAG запуска скрипта обучения модели: [validate_model_dag.py](dags/validate_model_dag.py)

# Результаты работы скрипта валидации в MLFlow

![MLFlow result](img/6_mlflow_result.png "Mlflow result")

# Результаты выполнения DAG airflow

![Airflow result](img/6_airflow_result.png "Airflow result")