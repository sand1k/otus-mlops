import os
import time
import json
import requests

import s3fs
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

s3_filepath = "mlops-hw/transformed_full/fraud_data_sorted.parquet/year=2019/month=11/day=10"
df = pd.read_parquet(
    f"s3a://{s3_filepath}",
    storage_options={
        "key"          : os.getenv("AWS_ACCESS_KEY_ID"),
        "secret"       : os.getenv("AWS_SECRET_ACCESS_KEY"),
        "client_kwargs": {
            'verify'      : True,
            'region_name' : os.environ['AWS_DEFAULT_REGION'],
            'endpoint_url': os.environ['MLFLOW_S3_ENDPOINT_URL']
        }
    }
)

ONLY_FRAUD = False
if ONLY_FRAUD:
    df = df[df['tx_fraud'] == 1]

df = df.sort_values(by=['ts'])

for index, row in df.iterrows():
    numerical_columns = [
        "transaction_id",
        "ts",
        "tx_amount",
        "is_weekend",
        "is_night",
        "customer_id_nb_tx_1day_window",
        "customer_id_avg_amount_1day_window",
        "customer_id_nb_tx_7day_window",
        "customer_id_avg_amount_7day_window",
        "customer_id_nb_tx_30day_window",
        "customer_id_avg_amount_30day_window",
        "terminal_id_nb_tx_1day_window",
        "terminal_id_risk_1day_window",
        "terminal_id_nb_tx_7day_window",
        "terminal_id_risk_7day_window",
        "terminal_id_nb_tx_30day_window",
        "terminal_id_risk_30day_window",
        "tx_fraud"
    ]
    
    print("===========================================================")
    print(row)
    row['ts'] = row['ts'].strftime('%Y-%m-%d %H:%M:%S')
    response = requests.post(url=os.environ['FRAUD_SERVICE_ENDPOINT'], json=row[numerical_columns].to_dict())
    print("-----------------------------------------------------------")
    try:
        print("Prediction:", json.loads(response.text)['prediction'])
    except:
        print("Exception!\nResponse:\n", response.text)
    print("===========================================================")
    time.sleep(1.0)