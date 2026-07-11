import json
from pathlib import Path

from pyspark.sql.functions import (
    col,
    concat,
    day,
    dayofweek,
    hour,
    md5,
    monotonically_increasing_id,
    month,
    regexp_extract,
    when,
    year,
)

from src.utils.spark_session import spark


class StarSchema:
    def __init__(self, logs_df):
        self.logs_df = logs_df
        self.fact_requests = None
        self.dim_timestamp = None
        self.dim_endpoint = None
        self.dim_status = None
        self.dim_host = None

    def build_time_dimension(self):
        # i need: year, month, day, hour, day_of_week and surrogate key
        # for my reference - {1:sunday, 2:monday, ..., 7:saturday}
        self.dim_timestamp = self.logs_df.select("timestamp").distinct()
        self.dim_timestamp = self.dim_timestamp.withColumn(
            "day_of_week", dayofweek(col("timestamp"))
        )
        self.dim_timestamp = self.dim_timestamp.withColumn("hour", hour(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("year", year(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("month", month(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("day", day(col("timestamp")))
        t = col("timestamp")
        self.dim_timestamp = self.dim_timestamp.withColumn(
            "is_business_hour",
            dayofweek(t).between(2, 6) & hour(t).between(9, 16),
        )
        self.dim_timestamp = self.dim_timestamp.withColumn(
            "period_id",
            year(t) * 10000 + month(t) * 100 + day(t),
        )
        self.dim_timestamp = self.dim_timestamp.withColumn("timestamp_key", md5(t.cast("string")))

    def build_status_dimension(self):
        json_path = Path(__file__).resolve().parent / "http-codes.json"
        with open(json_path) as file:
            raw = json.load(file)
        rows = [{"status": k, **v} for k, v in raw.items()]
        df_descriptions = spark.createDataFrame(rows)
        self.dim_status = self.logs_df.select("status").distinct()
        self.dim_status = self.dim_status.join(df_descriptions, on="status", how="left")
        self.dim_status = self.dim_status.withColumn("status", col("status").cast("int"))
        self.dim_status = self.dim_status.withColumn("status_key", md5(col("status").cast("string")))
        self.dim_status = self.dim_status.drop("code")

    def build_endpoint_dimension(self):
        # i need each unique endpoint and surrogate key
        self.dim_endpoint = self.logs_df.select("endpoint", "method").distinct()
        self.dim_endpoint = self.dim_endpoint.withColumn(
            "extracted", regexp_extract(col("endpoint"), r"/(.*?)/", 1)
        )
        self.dim_endpoint = self.dim_endpoint.withColumn(
            "endpt_key", md5(concat(col("endpoint"), col("method")))
        )

    def build_host_table(self):
        # i need: client hostnames/ip, and surrogate key
        self.dim_host = self.logs_df.select("host").distinct()
        self.dim_host = self.dim_host.withColumn("host_key", md5(col("host")))

    def build_fact_table(self):
        self.fact_requests = self.logs_df.select(
            monotonically_increasing_id().alias("request_id"),
            year(col("timestamp")).alias("year"),
            month(col("timestamp")).alias("month"),
            md5(concat(col("endpoint"), col("method"))).alias("endpt_key"),
            md5(col("status")).alias("status_key"),
            md5(col("host")).alias("host_key"),
            md5(col("timestamp").cast("string")).alias("timestamp_key"),
            col("bytes").cast("long"),
        )

    def write_parquet(self, path="data/curated/"):
        self.fact_requests.write.partitionBy("year", "month").mode("overwrite").parquet(
            f"{path}fact_requests"
        )
        self.dim_endpoint.write.mode("overwrite").parquet(f"{path}dim_endpoint")
        self.dim_timestamp.write.mode("overwrite").parquet(f"{path}dim_timestamp")
        self.dim_status.write.mode("overwrite").parquet(f"{path}dim_status")
        self.dim_host.write.mode("overwrite").parquet(f"{path}dim_host")

    def build(self):
        self.build_time_dimension()
        self.build_endpoint_dimension()
        self.build_status_dimension()
        self.build_host_table()
        self.build_fact_table()
