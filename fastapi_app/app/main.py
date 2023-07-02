from fastapi import FastAPI
import uvicorn
from app.schemas import Transaction
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from fastapi.encoders import jsonable_encoder
import mlflow


app = FastAPI()


class Model:
    pipeline:  None


@app.on_event("startup")
def load_model():
    Model.pipeline = mlflow.pyfunc.load_model(model_uri=f"models:/fraud_classifier/Staging")


def train_model():
    df = pd.read_csv('app/heart_cleveland_upload.csv')
    X_train, X_test, y_train, y_test = train_test_split(df.drop('condition', axis=1),
                                                    df['condition'].values,
                                                    test_size=0.3,
                                                    random_state=1,
                                                    stratify=df['condition'].values)
    pipe = make_pipeline(StandardScaler(), LogisticRegression())
    pipe.fit(X_train, y_train)  
    Pipeline(steps=[('standardscaler', StandardScaler()),
                ('logisticregression', LogisticRegression())])
    score = pipe.score(X_test, y_test) 
    return pipe, score


@app.post("/predict/")
async def make_prediction(transaction: Transaction):
    df = pd.DataFrame(jsonable_encoder(transaction), index=[0])
    
    print("===========================================================")
    print("Object:")
    print("-----------------------------------------------------------")
    print(df)
    print(df.iloc[0])
    res = Model.pipeline.predict(df)
    print("-----------------------------------------------------------")
    print("Prediction:", res)
    print("===========================================================")
    return {"prediction": res}


@app.get("/")
async def root():
    return {"status": "Green"}


if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, host="0.0.0.0", reload=True)