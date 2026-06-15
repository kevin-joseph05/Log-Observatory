from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("Log Observatory")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
