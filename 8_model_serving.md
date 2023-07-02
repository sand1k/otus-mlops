# Обновление моделей

## Настройка окружения

## Скрипты генерации и обработки данных

Dockerfile для запуска fastapi сервиса: [Dockerfile](fastapi_app/Dockerfile).

Сборка и запуск докер контейнера вручную (переменные окружения AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, MLFLOW_TRACKING_URI и AWS_DEFAULT_REGION должны быть настроены в .env файле):
```
cd fastapi_app
docker build -t fraud_detection_img .
docker run --rm --name fraud_detection_container -p 8000:8000 --env-file ../.env fraud_detection_img
```

Запуск тестов (контейнер должен быть запущен):
```
docker exec -it fraud_detection_container bash
conda activate mlflow-env
pytest -v
```

Скрипт для генерации запросов к сервису: [generate_requests.py](scripts/generate_requests.py).
Запуск скрипта:
```
python scripts/generate_requests.py
```

## Обработка данных с выводом в консоль