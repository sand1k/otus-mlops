from datetime import datetime

import pyspark
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col, when

def calculate_accuracy(predictions):
    predictions = predictions.withColumn(
        "fraudPrediction",
        when((predictions.tx_fraud==1) & (predictions.prediction==1), 1).otherwise(0)
    )

    accurateFraud = predictions.groupBy("fraudPrediction").count().where(predictions.fraudPrediction==1).head()[1]
    totalFraud = predictions.groupBy("tx_fraud").count().where(predictions.tx_fraud==1).head()[1]
    accuracy = (accurateFraud/totalFraud)*100
    return accuracy

# Main
if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder
            #.config('spark.executor.instances', 8)
            .config("spark.executor.cores", 4)
            .appName("fraud_data_validate")
            .getOrCreate()
    )
    
    df = spark.read.parquet("/user/transformed_full/")
    df_validate = df.filter(col('ts').between("2019-10-28", "2019-11-05"))
    
    # Prepare MLFlow experiment for logging
    client = MlflowClient()
    experiment = client.get_experiment_by_name("Fraud_Data_Validate")
    experiment_id = experiment.experiment_id

    run_name = 'Fraud_data_validate' + ' ' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        # Load models
        model_latest = mlflow.spark.load_model(model_uri=f"models:/fraud_classifier/latest")
        model_staging = mlflow.spark.load_model(model_uri=f"models:/fraud_classifier/Staging")
        evaluator = BinaryClassificationEvaluator(labelCol='tx_fraud', rawPredictionCol='prediction')

        # Perform inference via model.transform()
        predictions_latest = model_latest.transform(df_validate)
        areaUnderROC_latest = evaluator.evaluate(predictions_latest)
        accuracy_latest = calculate_accuracy(predictions_latest)
        predictions_staging = model_staging.transform(df_validate)
        areaUnderROC_staging = evaluator.evaluate(predictions_staging)
        accuracy_staging = calculate_accuracy(predictions_staging)

        run_id = mlflow.active_run().info.run_id
        print(f"Logging metrics to MLflow run {run_id} ...")
        mlflow.log_metric("ROC-latest", areaUnderROC_latest)
        mlflow.log_metric("Acc-latest", accuracy_latest)
        print(f"Model ROC-latest: {areaUnderROC_latest}")
        print(f"Model Acc-latest: {accuracy_latest}")
        
        mlflow.log_metric("ROC-staging", areaUnderROC_staging)
        mlflow.log_metric("Acc-staging", accuracy_staging)
        print(f"Model ROC-staging: {areaUnderROC_staging}")
        print(f"Model Acc-staging: {accuracy_staging}")
        
    spark.stop()