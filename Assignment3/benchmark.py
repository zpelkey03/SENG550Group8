import os
import sys
import time
from pathlib import Path

import json
from datetime import datetime

from dotenv import load_dotenv
import shutil
import psycopg2
from psycopg2.extensions import connection as PGConnection

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

import matplotlib.pyplot as plt
import numpy as np

import csv

import pandas as pd

PG_QUERIES: dict[str, str] = {
    "Q1_revenue_by_country": """
        SELECT c.country, SUM(f.sales) as total_sales
        FROM fact_sales_scaled f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.country
        ORDER BY total_sales DESC;
    """,
    "Q2_top10_product_lines": """
        SELECT p.product_line, SUM(f.sales) AS total_sales
        FROM fact_sales_scaled f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_line
        ORDER BY total_sales DESC
        LIMIT 10;
    """,
    "Q3_monthly_sales": """
        SELECT d.year_id, d.month_id, d.month_name, SUM(f.sales) AS total_sales
        FROM fact_sales_scaled f
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

PLOTS_DIR = Path("plots")
PARQUETS_DIR = Path("dataset_parquets")

def load_env() -> None:
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path)
    print(f"Using .env at: {env_path.resolve()}")
    
    # Set up PySpark environment variables to avoid Python executable issues with Spark
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    
    # Set JAVA_HOME to use Java 21 for PySpark
    os.environ["JAVA_HOME"] = r"C:\Zulu\zulu-21"
    
    print(f"Set PYSPARK_PYTHON to: {python_executable}")
    print(f"Set PYSPARK_DRIVER_PYTHON to: {python_executable}")
    print(f"Set JAVA_HOME to: {os.environ['JAVA_HOME']}")

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
    
    spark.sparkContext.setLogLevel("WARN")

    print("Spark Session Created:", spark.version)
    return spark

def prescale_spark_df(spark: SparkSession, csv_path: str, scales: list[int]):
    """ Create scaled Spark Dataframesas Parquet files for use across multiple runs

    Args:
        spark (SparkSession): Spark API
        csv_path (str): CSV to be read for creating dataframe
        scales (list[int]): list of different scales to create
    """
    
    print("Creating Persistent datasets")
    base_df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    for scale in scales:
        output_path = PARQUETS_DIR / f"scaled_data_{scale}.parquet"
        if Path(output_path).exists():
            print(f"Scale {scale} dataset already exists, skipping")
            continue
        
        print(f" Creating scale {scale} dataset")
        if scale == 1:
            scaled = base_df
        else:
            reps = spark.range(scale).withColumnRenamed("id", "_rep")
            scaled = base_df.crossJoin(reps).select(*base_df.columns)
            
        scaled.write.mode("overwrite").parquet(str(output_path))
        print(f"Scale {scale} dataset saved to {output_path}")
        
    print("All spark dataset scaling complete")

def load_scaled_spark_df(spark: SparkSession, scale_factor: int) -> DataFrame:
    """
    
    Read Spark Dataframe from parquet file

    Args:
        spark (SparkSession): SparkSession API object to manage dataframe
        csv_path (str): path for dataset
        scale_factor (int, optional): multiples row count by specified factor. Defaults to 1.

    Returns:
        DataFrame: spark dataframe after scaling
    """
    path = PARQUETS_DIR / f"scaled_data_{scale_factor}.parquet"
    
    if not Path(path).exists():
        raise FileNotFoundError(f"Scaled dataset not found: {path}")
    
    df = spark.read.parquet(str(path))
    cores = os.cpu_count() or 4
    df = df.repartition(max(8, cores * 2))
    
    df = df.cache()
    df.count()
    
    return df

def scale_postgresql_data(conn: PGConnection, scale_factor: int):
    """Scales fact_sales as new table in postgres according to scale_factor

    Args:
        conn (PGConnection): database connection
        scale_factor (int): scale factor to be used
    """
    print(f"Scaling PostgreSQL data by {scale_factor}x")
    
    with conn.cursor() as cur:
        
        cur.execute("DROP TABLE IF EXISTS fact_sales_scaled CASCADE;")
        
        cur.execute("CREATE TABLE fact_sales_scaled AS SELECT * FROM fact_sales;")
        
        if (scale_factor == 1):
            cur.execute("INSERT INTO fact_sales_scaled SELECT * FROM fact_sales;")
            pass
        else:
            for i in range(scale_factor - 1):
                cur.execute("INSERT INTO fact_sales_scaled SELECT * FROM fact_sales;")
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM fact_sales_scaled;")
        row_count = cur.fetchone()[0]
        print(f"PostgreSQL scaled table now has {row_count:,} rows")
        
def get_pg_storage_size(scale_factor: int) -> float:
    conn = get_pg_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT pg_total_relation_size('fact_sales_scaled')")
    total_bytes = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return total_bytes / (1024 * 1024)

def get_spark_storage_size(scale_factor: int) -> float:
    parquet_path = PARQUETS_DIR / f"scaled_data_{scale_factor}.parquet"
    
    if not parquet_path.exists():
        return 0.0
    
    total_size = sum(
        f.stat().st_size
        for f in parquet_path.rglob('*')
        if f.is_file()
    )
    
    return total_size / (1024 * 1024)

def plot_per_run_results_bar_plot(pg_results, spark_results, spark_scale):
    
    # gather querie naems for plot and initialize list of times
    queries = list(PG_QUERIES.keys())
    pg_times = [pg_results.get(q, float('nan')) for q in queries]
    sp_times = [spark_results.get(q, float('nan')) for q in queries]

    indices = np.arange(len(queries))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.bar(indices - width/2, pg_times, width, label='PostgreSQL', color='C0')
    ax.bar(indices + width/2, sp_times, width, label='Spark', color='C1')
    ax.set_xticks(indices)
    ax.set_xticklabels(queries, rotation=25, ha='right')
    ax.set_ylabel('Average time (s)')
    ax.set_title(f"Benchmark times (scale={spark_scale})")
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    plt.tight_layout()
    out = PLOTS_DIR / f"benchmark_times_scale_{spark_scale}.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    
def plot_scaling_curves(all_runs: dict):
    """
    Generates a curve plot comparing the execution times of Postgres and Spark

    Args:
        all_runs (dict): dictionary to plot data with
    """
    if not all_runs:
        print("No runs provided to plot_scaling_curves.")
        return
    
    scales = sorted(all_runs.keys())
    queries = list(PG_QUERIES.keys())
    
    fig, axes = plt.subplots(len(queries), 1, figsize=(10, 4 * len(queries)), sharex=True)
    if len(queries) == 1:
        axes = [axes]
        
    for i, q in enumerate(queries):
        pg_times = [all_runs[s]["postgresql"].get(q, float('nan')) for s in scales]
        sp_times = [all_runs[s]["spark"].get(q, float('nan')) for s in scales]
        
        ax = axes[i]
        ax.plot(scales, pg_times, marker='o', linestyle='-', label="PostgreSQL", color='C0')
        ax.plot(scales, sp_times, marker='s', linestyle='--', label='Spark', color='C1')
        
        ax.set_xscale('log')
        ax.set_title(q)
        ax.set_ylabel('Time (s)')
        ax.grid(True, which='both', ls='--', alpha=0.3)
        ax.legend()
        
    axes[-1].set_xlabel('Scale (multiplier)')
    plt.tight_layout()
    out = PLOTS_DIR / "benchmark_scaling_curves.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved scaling curves to {out}")
    
def plot_storage_comparison(all_runs: dict):
    
    if not all_runs:
        print("No runs to plot storage comparison")
        return
    
    scales = sorted(all_runs.keys())
    pg_sizes = [all_runs[s]["storage"]["postgresql_mb"] for s in scales]
    spark_sizes = [all_runs[s]["storage"]["spark_mb"] for s in scales]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Plot absolute storage sizes
    ax1.plot(scales, pg_sizes, marker='o', label='PostgreSQL', color='C0', linewidth=2)
    ax1.plot(scales, spark_sizes, marker='s', label='Spark (Parquet)', color='C1', linewidth=2)
    
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax1.set_xlabel('Scale')
    ax1.set_ylabel('Storage Size (MB)')
    ax1.set_title('Storage Size Comparison')
    ax1.grid(True, which='both', ls='--', alpha=0.3)
    ax1.legend()
    
    #Plot storage effiency ratio
    ratios = [pg / spark if spark > 0 else 0 for pg, spark in zip(pg_sizes, spark_sizes)]
    
    ax2.plot(scales, ratios, marker='D', color='C2', linewidth=2)
    ax2.set_xscale('log')
    ax2.set_xlabel('Scale')
    ax2.set_ylabel('Storage Ratio (PostgreSQL / Spark)')
    ax2.set_title('Storage Efficiency (High = Parquet More Efficient)')
    ax2.grid(True, which='both', ls='--', alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    out = PLOTS_DIR / "storage_comparison.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved storage comparison to {out}")
    
    
def export_all_runs_to_csv(all_runs: dict):
    if not all_runs:
        print("No runs to export.")
        return
    
    rows = []
    
    for scale, data in all_runs.items():
        pg_storage = data["storage"]["postgresql_mb"]
        spark_storage = data["storage"]["spark_mb"]
        storage_ratio = pg_storage / spark_storage if spark_storage > 0 else 0
        
        for query_name in PG_QUERIES.keys():
            row = {
                "scale": scale,
                "query": query_name,
                "pg_time_s": data["postgresql"][query_name],
                "spark_time_s": data["spark"][query_name],
                "speedup": data["postgresql"][query_name] / data["spark"][query_name],
                "pg_storage_mb": pg_storage,
                "spark_storage_mb": spark_storage,
                "storage_ratio": storage_ratio
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_csv("benchmark_all_runs.csv", index=False)
    print(f"Exported all runs to benchmark_all_runs.csv")

def run_pg_benchmark(scale_factor):
    print("\n--- PostgreSQL Benchmarks ---\n")
    conn = get_pg_connection()
    try:
        scale_postgresql_data(conn, scale_factor)

        scaled_pg_queries = PG_QUERIES

        pg_results: dict[str, float] = {}
        for name, sql in scaled_pg_queries.items():
            avg_time = time_pg_query(conn, sql, repeats=3)
            pg_results[name] = avg_time
            print(f"{name}: {avg_time:.4f} s (avg of 3 runs)")
    finally:
        conn.close()
        
    return pg_results

def run_spark_benchmark(spark: SparkSession, scale_factor: int):
    print("\n--- Spark Benchmarks ---\n")
    try:
        df: DataFrame = load_scaled_spark_df(spark, scale_factor)
        df.createOrReplaceTempView("sales_data")

        spark_results: dict[str, float] = {}
        for name, sql in SPARK_QUERIES.items():
            avg_time = time_spark_squery(spark, sql, repeats=3)
            spark_results[name] = avg_time
            print(f"{name}: {avg_time:.4f} s (avg of 3 runs)")
    finally:
        pass
        
    return spark_results

def print_benchmark_summary(pg_results, spark_results, scale_factor):
    print(f"\n--- Summary (scale={scale_factor}) ---")
    print(f"{'Query':30s}\t{'PostgreSQL':>12s}\t {'Spark':>12s}")
    print("-" * 60)
    for name in PG_QUERIES.keys():
        pg_t = pg_results.get(name, float("nan"))
        sp_t = spark_results.get(name, float("nan"))
        print(f"{name:30s}\t{pg_t:12.4f}\t{sp_t:12.4f}")

def print_storage_efficiency_summary(all_runs: dict):
    scales = sorted(all_runs.keys())
    
    print("\n--- Storage Efficiency Summary ---")
    print("-"*60)
    print(f"{'Scale':<10} {'PostgreSQL (MB)':<20} {'Spark (MB)':<15} {'Ratio':<10}")
    print("-"*60)
    for scale in scales:
        pg_size = all_runs[scale]["storage"]["postgresql_mb"]
        spark_size = all_runs[scale]["storage"]["spark_mb"]
        ratio = pg_size / spark_size if spark_size > 0 else 0
        print(f"{scale:<10} {pg_size:<20.2f} {spark_size:<15.2f} {ratio:<10.2f}x")

def main():
    load_env()

    # Load csv path from .env
    csv_path = os.getenv("CSV_PATH")
    if not csv_path:
        raise RuntimeError("CSV_PATH not set in environment file")
    
    if PLOTS_DIR.exists():
        shutil.rmtree(PLOTS_DIR)
    
    PLOTS_DIR.mkdir(exist_ok=True)
    PARQUETS_DIR.mkdir(exist_ok=True)

    # set scales to be used for benchmarking and create dictionary to store each run
    scales = [1, 10, 100, 1000, 10000, 15000, 20000]
    all_runs = {}

    spark = create_spark_session()

    prescale_spark_df(spark, csv_path, scales)

    for scale_factor in scales:
        
        pg_results = run_pg_benchmark(scale_factor)
        spark_results = run_spark_benchmark(spark, scale_factor)
        
        pg_storage_mb = get_pg_storage_size(scale_factor)
        spark_storage_mb = get_spark_storage_size(scale_factor)

        all_runs[scale_factor] = {
            "postgresql": pg_results,
            "spark": spark_results,
            "storage": {
                "postgresql_mb": pg_storage_mb,
                "spark_mb": spark_storage_mb
            }
            }

        print_benchmark_summary(pg_results, spark_results, scale_factor)
        plot_per_run_results_bar_plot(pg_results, spark_results, scale_factor)
        
        spark.catalog.clearCache()
    
    export_all_runs_to_csv(all_runs)
         
    plot_scaling_curves(all_runs)
    plot_storage_comparison(all_runs)
    print_storage_efficiency_summary(all_runs)
    
    print("Benchmarking complete")
    
    spark.stop()


if __name__ == "__main__":
    main()
