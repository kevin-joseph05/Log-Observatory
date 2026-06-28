import json
from pathlib import Path

from pyspark.sql.functions import (
    col,
    day,
    dayofweek,
    hour,
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
        self.dim_timestamp = self.dim_timestamp.withColumn("day_of_week", dayofweek(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("hour", hour(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("year", year(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("month", month(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("day", day(col("timestamp")))
        self.dim_timestamp = self.dim_timestamp.withColumn("timestamp_key", md5(col("timestamp").cast("string")))

    def build_status_dimension(self):
        json_path = Path(__file__).resolve().parent / "http-codes.json"
        with open(json_path) as file:
            raw = json.load(file)
        rows = [{"status": k, **v} for k, v in raw.items()]
        df_descriptions = spark.createDataFrame(rows)
        self.dim_status = self.logs_df.withColumn("status", col("status").cast("int"))
        self.dim_status = self.dim_status.withColumn(
            "status",
            when((col("status") >= 200) & (col("status") < 300), "2xx")
            .when((col("status") >= 300) & (col("status") < 400), "3xx")
            .when((col("status") >= 400) & (col("status") < 500), "4xx")
            .when((col("status") >= 500) & (col("status") < 600), "5xx")
            .otherwise("Unknown")
        )
        self.dim_status = self.dim_status.join(df_descriptions, on="status", how="left")
        self.dim_status = self.dim_status.withColumn("status_key", md5(col("status")))

    def build_endpoint_dimension(self):
        # i need each unique endpoint and surrogate key
        self.dim_endpoint = self.logs_df.select("endpoint").distinct()
        self.dim_endpoint = self.dim_endpoint.withColumn("extracted", regexp_extract(col("endpoint"), r'/(.*?)/', 1))
        self.dim_endpoint = self.dim_endpoint.withColumn("endpt_key", md5(col("endpoint")))


    def build_host_table(self):
        #i need: client hostnames/ip, and surrogate key
        self.dim_host = self.logs_df.select("host").distinct()
        self.dim_host = self.dim_host.withColumn("host_key", md5(col("host")))

    def build_fact_table(self):
        # i don't know what the difference is between this and the raw table
        # over here, i would need to join all the individual tables to the fact table
        self.fact_requests = self.logs_df.select(
                monotonically_increasing_id().alias("request_id"),
                md5(col("endpoint")).alias("endpt_key"),
                md5(
                    when((col("status") >= 200) & (col("status") < 300), "2xx")
                    .when((col("status") >= 300) & (col("status") < 400), "3xx")
                    .when((col("status") >= 400) & (col("status") < 500), "4xx")
                    .when((col("status") >= 500) & (col("status") < 600), "5xx")
                    .otherwise("Unknown")
                ).alias("status_key"),
                md5(col("host")).alias("host_key"),
                md5(col("timestamp").cast("string")).alias("timestamp_key"),
                col("bytes").cast("long"),


        )

    def build(self):
        pass


