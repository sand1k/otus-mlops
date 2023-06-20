import findspark
findspark.init() 

import subprocess
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import (
    hour,
    dayofweek,
    year,
    month,
    dayofmonth,
    count,
    to_timestamp,
    when,
    isnan,
    col,
    udf
)
from pyspark.sql.functions import sum, avg, count, col
from pyspark.sql.window import Window

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


# Define a UDF to handle the special case
def convert_timestamp(s):
    if s[11:13] == '24':
        return s[:11] + '00' + s[13:]
    return s


def is_night(tx_hour):
    is_night = tx_hour <= 6
    return int(is_night)


# Define function
def get_customer_spending_behaviour_features(transactions_df, windows_size_in_days=[1,7,30]):
    # For each window size
    for window_size in windows_size_in_days:
        # Define the window
        window = Window \
            .partitionBy("customer_id") \
            .orderBy(col("ts").cast("long")) \
            .rangeBetween(-window_size * 86400, 0)  # 86400 is number of seconds in a day

        # Compute the sum of the transaction amounts, the number of transactions and the average transaction amount for the given window size
        transactions_df = transactions_df \
            .withColumn('nb_tx_window', count("tx_amount").over(window)) \
            .withColumn('avg_amount_tx_window', avg("tx_amount").over(window))

        # Rename the columns
        transactions_df = transactions_df \
            .withColumnRenamed('nb_tx_window', 'customer_id_nb_tx_' + str(window_size) + 'day_window') \
            .withColumnRenamed('avg_amount_tx_window', 'customer_id_avg_amount_' + str(window_size) + 'day_window')

    # Return the dataframe with the new features
    return transactions_df


def get_count_risk_rolling_window(transactions_df, delay_period=7, windows_size_in_days=[1,7,30], precision_loss=1):
    # Define the delay window
    delay_window = Window \
        .partitionBy("terminal_id") \
        .orderBy(col("truncated_ts").cast("long")) \
        .rangeBetween(-delay_period * 86400 // precision_loss, 0)  

    # Compute the number of frauds and transactions for the delay window
    transactions_df = transactions_df \
        .withColumn('nb_fraud_delay', sum("tx_fraud").over(delay_window)) \
        .withColumn('nb_tx_delay', count("tx_fraud").over(delay_window)) 

    for window_size in windows_size_in_days:
        # Define the window including the delay period and the window size
        delay_window_size = Window \
            .partitionBy("terminal_id") \
            .orderBy(col("truncated_ts").cast("long")) \
            .rangeBetween(-(delay_period + window_size) * 86400 // precision_loss, 0)

        # Compute the number of frauds and transactions for the delay window size
        transactions_df = transactions_df \
            .withColumn('nb_fraud_delay_window', sum("tx_fraud").over(delay_window_size)) \
            .withColumn('nb_tx_delay_window', count("tx_fraud").over(delay_window_size)) 

        # Compute the number of frauds and transactions for the window size
        transactions_df = transactions_df \
            .withColumn('nb_fraud_window', col('nb_fraud_delay_window') - col('nb_fraud_delay')) \
            .withColumn('nb_tx_window', col('nb_tx_delay_window') - col('nb_tx_delay')) 

        # Compute the risk for the window size
        transactions_df = transactions_df \
            .withColumn('risk_window', when(col('nb_tx_window') > 0, col('nb_fraud_window') / col('nb_tx_window')).otherwise(0))

        # Rename the columns
        transactions_df = transactions_df \
            .withColumnRenamed('nb_tx_window', 'terminal_id_nb_tx_' + str(window_size) + 'day_window') \
            .withColumnRenamed('risk_window', 'terminal_id_risk_' + str(window_size) + 'day_window') 

    return transactions_df

        
def preprocess(df_orig):
    df = df_orig.na.drop()
    dropped_cnt = df_orig.count() - df.count()
    print(f"Dropped {dropped_cnt} rows with null values.")
    
    convert_timestamp_udf = udf(convert_timestamp)
    df = df.withColumn("tx_datetime", convert_timestamp_udf(df["tx_datetime"]))
    df = df.withColumn("ts", to_timestamp(df["tx_datetime"], "yyyy-MM-dd HH:mm:ss"))
    
    # Extract new features from the tx_datetime column
    df = df.withColumn("year", year(df["ts"]))
    df = df.withColumn("month", month(df["ts"]))
    df = df.withColumn("day", dayofmonth(df["ts"]))
    df = df.withColumn("is_weekend", dayofweek("ts").isin([1,7]).cast("int"))
    is_night_udf = udf(is_night, IntegerType())
    df = df.withColumn("is_night", is_night_udf(hour("ts")))
    
    return df


if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder
            #.config('spark.executor.instances', 8)
            .config("spark.executor.cores", 4)
            .appName("fraud_data_preproc")
            .getOrCreate()
    )
    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter
    
    new_files = get_files_list()
    print(new_files)
    
    df = read_files(new_files)
    
    # Calculate rows number
    row_count = df.count()
    print(f"Row count: {row_count}")
    
    # Find null values
    print("Check null values:")
    null_vals = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    null_vals.show()
    
    df = preprocess(df)
    
    # Order the dataframe
    df = df.orderBy('ts')
    df = get_customer_spending_behaviour_features(df, windows_size_in_days=[1, 7, 30])
    
    # Decrease the precision but calculate faster
    precision_loss = 3600
    df = df.withColumn("truncated_ts", (col("ts").cast("long") / precision_loss).cast("long"))
    df = get_count_risk_rolling_window(
        df,
        delay_period=7,
        windows_size_in_days=[1, 7, 30],
        precision_loss=precision_loss)
    
    # Save the transformed data as Parquet
    print("Writing data to HDFS...")
    output_path = "/user/transformed_full/"
    (df
         .write
         .partitionBy("year", "month", "day")
         .parquet(output_path, mode="overwrite")
    )
    print("Data saved.")
    spark.stop()
    