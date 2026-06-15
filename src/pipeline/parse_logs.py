from pyspark.sql.functions import regexp_extract

from src.utils.spark_session import spark

df = spark.read.text(["data/raw/access_log_Jul95", "data/raw/access_log_Aug95"])
pattern = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+(\S+)\s+\S+"\s+(\d{3})\s+(\d+|-)$'

df = df.select(
    regexp_extract("value", pattern, 1).alias("host"),
    regexp_extract("value", pattern, 2).alias("timestamp"),
    regexp_extract("value", pattern, 3).alias("method"),
    regexp_extract("value", pattern, 4).alias("endpoint"),
    regexp_extract("value", pattern, 5).alias("status"),
    regexp_extract("value", pattern, 6).alias("bytes"),
)
print(df.filter(df.host == "").count())

# df.show()
# df.printSchema()
