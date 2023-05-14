from datetime import datetime
import subprocess
import pyspark
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import countDistinct
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import hour, dayofweek, year, month, count, to_timestamp, when, isnan


def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err 

def get_files_list():        
    # Get file list from hdfs
    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-ls', '-C', '/user/fraud-data'])
    hdfs_files = [line for line in out.decode().split('\n') if len(line)]
    hdfs_files.sort()
    return hdfs_files


def read_files(file_list):
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("tx_datetime", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True)
    ])

    # Load the CSV file into a DataFrame
    df = (spark.read
        .format("csv")
        .schema(schema)
        .option("header", False)
        .option("sep", ',')
        .option("comment", '#')
        .load(file_list)
    )
    return df

def explore(df):
    # Show the DataFrame
    df.show()
    
    row_count = df.count()
    print(f"Row count: {row_count}")
    
    # Count the number of NaN values in each column
    print("Count of NaN values in each column:")
    nan_count = df.select([count(when(isnan(c), c)).alias(c) for c in df.columns])
    nan_count.show()
    
    #for column in ["customer_id", "terminal_id", "tx_fraud_scenario"]:
    #    distinct_count = df.agg(countDistinct(column)).collect()[0][0]
    #    print(f"{column}: {distinct_count} unique values")
    return df

def preprocess(df):
    # Convert the tx_datetime column to a timestamp type
    df = df.withColumn("tx_datetime", to_timestamp(df["tx_datetime"], "yyyy-MM-dd HH:mm:ss"))
    
    # Extract new features from the tx_datetime column
    df = df.withColumn("year", year(df["tx_datetime"]))
    df = df.withColumn("month", month(df["tx_datetime"]))
    df = df.withColumn("hour", hour(df["tx_datetime"]))
    df = df.withColumn("day_of_week", dayofweek(df["tx_datetime"]))
        
    return df
        
def build_train_pipeline():
    # Define the pipeline for feature extraction and transformation
    # Here, we'll use StringIndexer and OneHotEncoder for categorical features.
    # For numerical features, we'll use StandardScaler to scale them.
    # Define the pipeline stages
    stages = []

    # Terminal_id, hour_of_day, and day_of_week should be treated as categorical features
    categorical_columns = ["hour", "day_of_week", "month"]
    for column in categorical_columns:
        string_indexer = StringIndexer(inputCol=column, outputCol=f"{column}_index", handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=[string_indexer.getOutputCol()], outputCols=[f"{column}_ohe"])
        stages += [string_indexer, encoder]

    # Define numerical columns and apply StandardScaler
    numerical_columns = ["tx_amount"]
    for column in numerical_columns:
        vector_assembler = VectorAssembler(inputCols=[column], outputCol=f"{column}_vec")
        scaler = StandardScaler(inputCol=vector_assembler.getOutputCol(), outputCol=f"{column}_scaled", withStd=True, withMean=True)
        stages += [vector_assembler, scaler]

    # Combine all the transformed features into a single "features" column
    assembler_input = [f"{column}_ohe" for column in categorical_columns] + [f"{column}_scaled" for column in numerical_columns] 
    #assembler_input = [f"{column}_scaled" for column in numerical_columns] 
    vector_assembler = VectorAssembler(inputCols=assembler_input, outputCol="features")
    stages += [vector_assembler]
    
    # Add model
    regression = LinearRegression(featuresCol='features', labelCol='tx_fraud')
    stages += [regression]

    # Create the pipeline
    pipeline = Pipeline(stages=stages)
    
    return pipeline


# Main

spark = (
    pyspark.sql.SparkSession.builder
        .appName("fraud_data_train")
        .getOrCreate()
)
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

# Read available files
new_files = get_files_list()
new_files

df = read_files(new_files[0])
df = preprocess(df)

# Prepare MLFlow experiment for logging
client = MlflowClient()
experiment = client.get_experiment_by_name("Fraud_Data")
experiment_id = experiment.experiment_id

run_name = 'Fraud_data_pipeline' + ' ' + str(datetime.now())

with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    inf_pipeline = build_train_pipeline()
    
    train, test = df.randomSplit([0.9, 0.1], seed=12345)

    print("Fitting new model / inference pipeline ...")
    model = inf_pipeline.fit(train)

    print("Scoring the model ...")
    evaluator = BinaryClassificationEvaluator(labelCol='tx_fraud', rawPredictionCol='prediction')
    
    predictions_train = model.transform(train)
    areaUnderROC_train = evaluator.evaluate(predictions_train)
    
    predictions_test = model.transform(test)
    areaUnderROC_test = evaluator.evaluate(predictions_test)

    run_id = mlflow.active_run().info.run_id
    print(f"Logging metrics to MLflow run {run_id} ...")
    mlflow.log_metric("areaUnderROC-train", areaUnderROC_train)
    print(f"Model areaUnderROC-train: {areaUnderROC_train}")
    mlflow.log_metric("areaUnderROC-test", areaUnderROC_test)
    print(f"Model areaUnderROC-test: {areaUnderROC_test}")

    print("Saving model ...")
    mlflow.spark.save_model(model, 'fraud_classifier')

    print("Exporting/logging model ...")
    mlflow.spark.log_model(model, 'fraud_classifier')
    print("Done")