from pydantic import BaseModel
from datetime import datetime

class Transaction(BaseModel):
    transaction_id: int
    ts: datetime
    tx_amount: float
    is_weekend: int
    is_night: int
    customer_id_nb_tx_1day_window: int
    customer_id_avg_amount_1day_window: float
    customer_id_nb_tx_7day_window: int
    customer_id_avg_amount_7day_window: float
    customer_id_nb_tx_30day_window: int
    customer_id_avg_amount_30day_window: float
    terminal_id_nb_tx_1day_window: int
    terminal_id_risk_1day_window: float
    terminal_id_nb_tx_7day_window: int
    terminal_id_risk_7day_window: float
    terminal_id_nb_tx_30day_window: int
    terminal_id_risk_30day_window: float
    tx_fraud: int