import os
import subprocess
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import countDistinct
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import hour, dayofweek, year, month, count, to_timestamp, when, isnan


spark = (
    pyspark.sql.SparkSession.builder
        .appName("Spark data preprocessing")
        .getOrCreate()
)

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
    (ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', '-C', '/user/fraud-data'])
    hdfs_files = [line for line in out.decode().split('\n') if len(line)]
    hdfs_files.sort()
    return hdfs_files

def read_files(file_list):
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
        
def preprocess_df(df):
    # Convert the tx_datetime column to a timestamp type
    df = df.withColumn("tx_datetime", to_timestamp(df["tx_datetime"], "yyyy-MM-dd HH:mm:ss"))
    
    # Extract new features from the tx_datetime column
    df = df.withColumn("year", year(df["tx_datetime"]))
    df = df.withColumn("month", month(df["tx_datetime"]))
    df = df.withColumn("hour", hour(df["tx_datetime"]))
    df = df.withColumn("day_of_week", dayofweek(df["tx_datetime"]))
    
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

    # Create the pipeline
    pipeline = Pipeline(stages=stages)
    
    # Fit the pipeline to the transactions DataFrame
    pipeline_model = pipeline.fit(df)

    # Transform the data
    df_transformed = pipeline_model.transform(df)
    
    # Show the transformed data
    df_transformed = df_transformed.select("year", "month", "features", "tx_fraud")

    df_transformed.show(truncate=False)
    
    return df_transformed
    

# read file list
files = get_files_list()

print("Processing files:")
print("\n".join(files))

df = read_files(files)


# transform
transformed_df = preprocess_df(df)


# Save the transformed data as Parquet
output_path = "/user/transformed/"
(transformed_df
     .write
     .partitionBy("year", "month")
     .parquet(output_path, mode="overwrite")
)

# Stop the SparkSession
spark.stop()