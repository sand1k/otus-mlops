import findspark
findspark.init()

import pyspark
from pyspark.sql.functions import col, to_json, struct

KAFKA_BOOTSTRAP_SERVERS = "rc1a-ifa7kf257ig8r7cm.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "transactions"

if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder
            .appName("spark_kafka_produce")
            .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    df = spark.read.parquet("/user/transformed_full/year=2019/month=11/day=10/")
    
    sample_count = 10
    sampled = df.sample(1.0*sample_count/df.count()).limit(sample_count)
    
    columns = [
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
    sampled = sampled.select(columns)

    df_kafka = sampled.select(
        col("transaction_id").cast("string"),
        to_json(struct([sampled[x] for x in sampled.columns]))
    ).toDF("key", "value")
    
    df_kafka.show()
    
    (df_kafka
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required username='<username>' password='<password>';"
        )
        .option("kafka.ssl.truststore.location", "/etc/security/ssl")
        .option("kafka.ssl.truststore.password", "<password>")
        .option("topic", KAFKA_TOPIC)
        .save()
    )
    
    spark.stop()
