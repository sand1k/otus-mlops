import pyspark

if __name__ == "__main__":
    spark = (
        pyspark.sql.SparkSession.builder
            #.config('spark.executor.instances', 8)
            .config("spark.executor.cores", 4)
            .appName("fraud_data_validate")
            .getOrCreate()
    )

    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "storage.yandexcloud.net")
    spark._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<access_key>")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<secret_key>")
    
    df = spark.read.parquet("/user/transformed_full/")
    s3_url = 's3a://mlops-hw/transformed_full/foo.parquet'
    df.write.parquet(
        s3_url,
        mode = 'overwrite',
        partitionBy=['year', 'month', 'day'])
    
    spark.stop()