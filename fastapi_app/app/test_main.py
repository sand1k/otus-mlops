import json
from fastapi.testclient import TestClient
from app.main import app


def test_healthcheck():
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["status"] == "Green"


def test_predict():
    with TestClient(app) as client:
        transaction = {
            "transaction_id": 126321480,
            "ts": "2019-11-10 00:02:19",
            "tx_amount": 32.7,
            "is_weekend": 1,
            "is_night": 1,
            "customer_id_nb_tx_1day_window": 4,
            "customer_id_avg_amount_1day_window": 40.3425,
            "customer_id_nb_tx_7day_window": 18,
            "customer_id_avg_amount_7day_window": 50.448889,
            "customer_id_nb_tx_30day_window": 69,
            "customer_id_avg_amount_30day_window": 55.98971,
            "terminal_id_nb_tx_1day_window": 1624,
            "terminal_id_risk_1day_window": 1.0,
            "terminal_id_nb_tx_7day_window": 10948,
            "terminal_id_risk_7day_window": 1.0,
            "terminal_id_nb_tx_30day_window": 46816,
            "terminal_id_risk_30day_window": 0.399949,
            "tx_fraud": 1
        }
        response = client.post("/predict/", json=transaction)
        assert response.status_code == 200
        assert response.json()["prediction"] == [1.0]
