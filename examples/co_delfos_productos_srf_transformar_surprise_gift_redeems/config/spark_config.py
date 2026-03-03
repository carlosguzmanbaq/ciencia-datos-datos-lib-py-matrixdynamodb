from pyspark.conf import SparkConf

# Spark configuration for DynamoDB ETL processing
# Customize these settings based on your environment and requirements
spark_config = (
    SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    # Add your custom configurations here:
    # .set("spark.executor.memory", "4g")
    # .set("spark.executor.cores", "2")
    # .set("spark.driver.memory", "2g")
)