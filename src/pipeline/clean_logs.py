from pyspark.sql.functions import col, regexp_replace, to_timestamp, when


class CleanLogs:
    def __init__(self, df):
        self.df = df

    def drop_failed_parse(self):
        self.df = self.df.filter(
            (col("host") != "")
            & (col("timestamp") != "")
            & (col("method") != "")
            & (col("endpoint") != "")
            & (col("status") != "")
        )

    def normalize_bytes(self):
        self.df = self.df.withColumn(
            "bytes", when(col("bytes") == "-", 0).otherwise(col("bytes").cast("int"))
        )

    def strip_query(self):
        self.df = self.df.withColumn("endpoint", regexp_replace("endpoint", r"\?.*$", ""))

    def cast_timestamp(self):
        self.df = self.df.withColumn(
            "timestamp", to_timestamp("timestamp", "dd/MMM/yyyy:HH:mm:ss Z")
        )

    def clean_parsed_df(self):
        self.drop_failed_parse()
        self.normalize_bytes()
        self.strip_query()
        self.cast_timestamp()
        return self.df


if __name__ == "__main__":
    pass
