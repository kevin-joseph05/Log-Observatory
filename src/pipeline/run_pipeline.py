from src.pipeline.parse_logs import parse_raw_log
from src.pipeline.clean_logs import CleanLogs
from src.schema.star_schema import StarSchema

df = parse_raw_log()
df = CleanLogs(df).clean_parsed_df()
ss = StarSchema(df)
ss.build()
ss.write_parquet()
