import findspark
findspark.init()

from datetime import datetime
import pyspark
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when


def build_train_pipeline():
    # Define the pipeline stages
    stages = []

    # Define numerical columns and apply StandardScaler
    numerical_columns = [
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
         "terminal_id_risk_30day_window"
    ]
    
    assembler_input = [column for column in numerical_columns] 
    vector_assembler = VectorAssembler(inputCols=assembler_input, outputCol="features")
    stages += [vector_assembler]
    
    # Add model
    #classification = RandomForestClassifier(featuresCol='features', labelCol='tx_fraud')
    classification = LogisticRegression(featuresCol='features', labelCol='tx_fraud')
    stages += [classification]

    # Create the pipeline
    pipeline = Pipeline(stages=stages)
    
    return pipeline


def calculate_accuracy(predictions):
    predictions = predictions.withColumn(
        "fraudPrediction",
        when((predictions.tx_fraud==1) & (predictions.prediction==1), 1).otherwise(0)
    )

    accurateFraud = predictions.groupBy("fraudPrediction").count().where(predictions.fraudPrediction==1).head()[1]
    totalFraud = predictions.groupBy("tx_fraud").count().where(predictions.tx_fraud==1).head()[1]
    accuracy = (accurateFraud/totalFraud)*100
    return accuracy


if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder
            #.config('spark.executor.instances', 8)
            .config("spark.executor.cores", 4)
            .appName("fraud_data_train")
            .getOrCreate()
    )
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter
    
    df = spark.read.parquet("/user/transformed_full/")
    df_train = df.filter(col('ts').between("2019-09-21", "2019-10-13"))
    df_test = df.filter(col('ts').between("2019-10-21", "2019-10-27"))

    client = MlflowClient()
    experiment = client.get_experiment_by_name("Fraud_Data")
    experiment_id = experiment.experiment_id

    run_name = 'Fraud_data_pipeline' + ' ' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        # Train model
        print("Fitting new model / inference pipeline ...")

        pipeline = build_train_pipeline()
        model = pipeline.fit(df_train)

        print("Scoring the model ...")
        evaluator = BinaryClassificationEvaluator(labelCol='tx_fraud', rawPredictionCol='prediction')

        predictions_train = model.transform(df_train)
        predictions_train.head()
        areaUnderROC_train = evaluator.evaluate(predictions_train)

        predictions_test = model.transform(df_test)
        areaUnderROC_test = evaluator.evaluate(predictions_test)

        run_id = mlflow.active_run().info.run_id
        print(f"Logging metrics to MLflow run {run_id} ...")
        mlflow.log_metric("ROC-train", areaUnderROC_train)
        print(f"Model ROC-train: {areaUnderROC_train}")
        mlflow.log_metric("ROC-test", areaUnderROC_test)
        print(f"Model ROC-test: {areaUnderROC_test}")

        print("Saving model locally...")
        model.write().overwrite().save("/user/models/latest.mdl")

        FraudPredictionAccuracy = calculate_accuracy(predictions_train)
        print("FraudPredictionAccuracy train:", FraudPredictionAccuracy)
        FraudPredictionAccuracy = calculate_accuracy(predictions_test)
        print("FraudPredictionAccuracy test:", FraudPredictionAccuracy)

        print("Exporting/logging model ...")
        mlflow.spark.log_model(model, 'fraud_classifier', registered_model_name='fraud_classifier')
        print("Done")
    
    spark.stop()