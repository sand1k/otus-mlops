import findspark
findspark.init()

import pyspark
import mlflow
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, TimestampType, IntegerType, StructField, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "rc1a-ifa7kf257ig8r7cm.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "transactions"
KAFKA_TOPIC_WRITE = "results"

if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder
            .appName("spark_streaming")
            .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    model = mlflow.spark.load_model(model_uri=f"models:/fraud_classifier/Staging")
    
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("ts", TimestampType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("is_night", IntegerType(), True),
        StructField("customer_id_nb_tx_1day_window", IntegerType(), True),
        StructField("customer_id_avg_amount_1day_window", DoubleType(), True),
        StructField("customer_id_nb_tx_7day_window", IntegerType(), True),
        StructField("customer_id_avg_amount_7day_window", DoubleType(), True),
        StructField("customer_id_nb_tx_30day_window", IntegerType(), True),
        StructField("customer_id_avg_amount_30day_window", DoubleType(), True),
        StructField("terminal_id_nb_tx_1day_window", IntegerType(), True),
        StructField("terminal_id_risk_1day_window", DoubleType(), True),
        StructField("terminal_id_nb_tx_7day_window", IntegerType(), True),
        StructField("terminal_id_risk_7day_window", DoubleType(), True),
        StructField("terminal_id_nb_tx_30day_window", IntegerType(), True),
        StructField("terminal_id_risk_30day_window", DoubleType(), True),
        StructField("tx_fraud", IntegerType(), True),
    ])

    df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            #.option("startingOffsets", "earliest")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username='<username>' password='<password>';"  # username and password for kafka user
            )
            .option("kafka.ssl.truststore.location", "/etc/security/ssl")
            .option("kafka.ssl.truststore.password", "<password>") # password for java keystore (used in command keytool -importcert ...)
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), schema).alias("data"))
            .select("data.*")
    )

    df.printSchema()
    
    results = model.transform(df)

    WRITE_TO_CONSOLE = False
    if WRITE_TO_CONSOLE:
        (results
             .select("transaction_id", "ts", "tx_fraud", "probability", "prediction")
             .writeStream
             .format("console")
             .outputMode("append")
             .start()
             .awaitTermination()
        )
    else:
        (results
            .select(
                col("transaction_id").cast("string"),
                to_json(struct([col(x) for x in ["transaction_id", "ts", "tx_fraud", "probability", "prediction"]]))
            ).toDF("key", "value")
            .writeStream
            .format("kafka")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username='<username>' password='<password>';" # username and password for kafka user
            )
            .option("kafka.ssl.truststore.location", "/etc/security/ssl")
            .option("kafka.ssl.truststore.password", "<password>") # password for java keystore (used in command keytool -importcert ...)
            .option("topic", KAFKA_TOPIC_WRITE)
            .start()
            .awaitTermination()
        )