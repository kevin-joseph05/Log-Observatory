import duckdb
from pathlib import Path
import sys


sys.path.insert(0, str(Path.cwd().parent))
project_root = Path.cwd()
con = duckdb.connect()
con.execute(f"""
    CREATE VIEW fact_requests AS
    SELECT * FROM read_parquet('{project_root}/output_dir/fact_requests/**/*.parquet')
""")

for dim in ['dim_endpoint', 'dim_host', 'dim_status', 'dim_timestamp']:
    con.execute(f"""
        CREATE VIEW {dim} AS
        SELECT * FROM read_parquet('{project_root}/output_dir/{dim}/*.parquet')
    """)

con.sql("SELECT COUNT(*) FROM fact_requests").show()

con.sql("""
        WITH weekly AS (
            SELECT 
                DATE_TRUNC('week', t.timestamp) AS week_start,
                COUNT(*) AS total, 
                SUM(CASE WHEN s.status IN ('4xx', '5xx') THEN 1 ELSE 0 END) AS errors
            FROM fact_requests as f
            JOIN dim_timestamp t ON f.timestamp_key = t.timestamp_key
            JOIN dim_status s ON f.status_key = s.status_key
            GROUP BY week_start
        )
        SELECT 
            week_start, total, errors,
            ROUND(errors*1.0 / total, 4) AS error_rate,
            ROUND((errors*1.0 / total) - LAG(errors*1.0 / 10) OVER (ORDER BY week_start), 4 ) AS wow_change
        FROM weekly
        ORDER BY week_start
        """)

con.sql("""
       SELECT 
            e.extracted AS endpoint_category,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.bytes) as p95_bytes
        FROM fact_requests as f
        JOIN dim_endpoint e ON f.endpt_key = e.endpt_key
        GROUP BY e.extracted
        """)

con.sql("""
        SELECT 
            e.endpoint, e.method,
            COUNT(*) AS total,
            SUM(CASE WHEN s.status IN ('4xx', '5xx') THEN 1 ELSE 0 END) AS errors,
            ROUND(errors*1.0 / total, 4) AS error_rate
        FROM fact_requests as f
        JOIN dim_endpoint e ON f.endpt_key = e.endpt_key
        JOIN dim_status s ON f.status_key = s.status_key
        GROUP BY e.endpoint, e.method
        ORDER BY error_rate DESC
        LIMIT 10

        """)


