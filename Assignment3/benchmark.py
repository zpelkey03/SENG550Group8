import os
import time
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection as PGConnection

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

PG_QUERIES: dict[str, str] = {
    "Q1_revenue_by_country": """
        SELECT c.country, SUM(f.sales) as total_sales
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.country
        ORDER BY total_sales DESC;
    """,
    "Q2_top10_product_lines": """
        SELECT p.product_line, SUM(f.sales) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_line
        ORDER BY total_sales DESC
        LIMIT 10;
    """,
    "Q3_monthly_sales": """
        SELECT d.year_id, d.month_id, d.month_name, SUM(f.sales) AS total_sales
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year_id, d.month_id, d.month_name
        ORDER BY d.year_id, d.month_id;
    """,
}

SPARK_QUERIES: dict[str, str] = {
    "Q1_revenue_by_country": """
        SELECT COUNTRY, SUM(SALES) AS total_sales
        FROM sales_data
        GROUP BY COUNTRY
        ORDER BY total_sales DESC
    """,
    "Q2_top10_product_lines": """
        SELECT PRODUCTLINE, SUM(SALES) AS total_sales
        FROM sales_data
        GROUP BY PRODUCTLINE
        ORDER BY total_sales DESC
        LIMIT 10
    """,
    "Q3_monthly_sales": """
        SELECT YEAR_ID, MONTH_ID, SUM(SALES) AS total_sales
        FROM sales_data
        GROUP BY YEAR_ID, MONTH_ID
    """,
}

def load_env() -> None:
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path)
    print(f"Using .env at: {env_path.resolve()}")
    
def get_pg_connection() -> PGConnection:
    
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
    return conn

def time_pg_query(conn: PGConnection, sql: str, repeats: int = 3) -> float:
    """
    Run a Sql query multiple times and return the average execution time in seconds

    Args:
        conn (PGConnection): database connection
        sql (str): query to be used
        repeats (int, optional): specifies amount of times to run query. Defaults to 3.

    Returns:
        float: average execution time in seconds
    """
    durations: list[float] = []
    with conn.cursor() as cur:
        for _ in range(repeats):
            start = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            end = time.perf_counter()
            durations.append(end - start)
    return sum(durations) / len(durations)

def create_spark_session() -> SparkSession:
    """
    Create spark app session for dataframe management

    Returns:
        SparkSession: Spark API object
    """
    spark: SparkSession = (
        SparkSession.builder
        .appName("SENG550_Benchmarking_App")
        .getOrCreate()
    )
    
    print("Spark Session Created:", spark.version)
    return spark

def load_scaled_spark_df(spark: SparkSession, csv_path: str, scale_factor: int = 1) -> DataFrame:
    """
    
    Load the CSV into a spark dataframe and optionally duplicate rows to
    simulate a larger dataset.
    
    scale_factor number is how many rows are multiplied,
    so scale_factor = 4 is 4x the amount of rows

    Args:
        spark (SparkSession): SparkSession API object to manage dataframe
        csv_path (str): path for dataset
        scale_factor (int, optional): multiples row count by specified factor. Defaults to 1.

    Returns:
        DataFrame: spark dataframe after scaling
    """
    
    df: DataFrame = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
    )
    
    if scale_factor <= 1:
        df_scaled: DataFrame = df
    else:
        df_scaled = df
        for _ in range(scale_factor - 1):
            df_scaled = df_scaled.unionByName(df)
            
    df_scaled = df_scaled.cache()
    df_scaled.count()
    return df_scaled

def time_spark_squery(spark: SparkSession, sql: str, repeats: int = 3) -> float:
    """
    Run a spark query multiple times and return the average execution time in seconds

    Args:
        spark (SparkSession): SparkSession API object to manage dataframe
        sql (str): query used
        repeats (int, optional): specifies amount of times to run query. Defaults to 3.

    Returns:
        float: average execution time in seconds
    """
    durations: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        spark.sql(sql).collect()
        end = time.perf_counter()
        durations.append(end - start)
    return sum(durations) / len(durations)

def main():
    load_env()
    
    csv_path = os.getenv("CSV_PATH")
    if not csv_path:
        raise RuntimeError("CSV_PATH not set in environment file")
    
    spark_scale = int(os.getenv("SPARK_SCALE", "4"))
    
    print(f"CSV_PATH = {csv_path}")
    print(f"SPARK_SCALE = {spark_scale}")
    
    print("\n--- PostgreSQL Benchmarks ---\n")
    
    conn = get_pg_connection()
    try:
        pg_results: dict[str, float] = {}
        for name, sql in PG_QUERIES.items():
            avg_time = time_pg_query(conn, sql, repeats=3)
            pg_results[name] = avg_time
            print(f"{name}: {avg_time:.4f} s (avg of 3 runs)")
    finally:
        conn.close()
        
    print("\n--- Spark Benchmarks ---\n")
    spark: SparkSession = create_spark_session()
    try:
        df: DataFrame = load_scaled_spark_df(spark, csv_path)
        df.createOrReplaceTempView("sales_data")
        
        spark_results: dict[str, float] = {}
        for name, sql in SPARK_QUERIES.items():
            avg_time = time_spark_squery(spark, sql, repeats=3)
            spark_results[name] = avg_time
            print(f"{name}: {avg_time:.4f} s (avg of 3 runs)")
    finally:
        spark.stop()

    print("\n--- Summary (average in seconds) ---")
    print(f"{'Query':30s}\t{'PostgreSQL':>12s}\t {'Spark':>12s}")
    print("-" * 60)
    for name in PG_QUERIES.keys():
        pg_t = pg_results.get(name, float("nan"))
        sp_t = spark_results.get(name, float("nan"))
        print(f"{name:30s}\t{pg_t:12.4f}\t{sp_t:12.4f}")
        

if __name__ == "__main__":
    main()