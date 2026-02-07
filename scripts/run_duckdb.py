"""
DuckDB baseline for TPC-H Query 1
Run with: python scripts/run_duckdb.py <path_to_lineitem.parquet> [--runs N]
"""
import sys
import time
import statistics
import duckdb

def run_tpch_q1(data_path: str, num_runs: int = 10):
    # Connect and configure for single-threaded execution
    con = duckdb.connect()
    con.execute("SET threads = 1")
    
    query = f"""
    SELECT
        l_returnflag,
        l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity) AS avg_qty,
        avg(l_extendedprice) AS avg_price,
        avg(l_discount) AS avg_disc,
        count(*) AS count_order
    FROM read_parquet('{data_path}')
    WHERE l_shipdate <= CAST('1998-09-02' AS date)
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus;
    """
    
    print("DuckDB TPC-H Query 1 Benchmark")
    print("=" * 40)
    print(f"Data path: {data_path}")
    print(f"Threads: 1 (single-threaded)")
    print()
    
    # Warmup
    print("Warmup run...")
    con.execute(query).fetchall()
    print()
    
    # Benchmark runs
    times = []
    result = None
    
    for i in range(1, num_runs + 1):
        start = time.perf_counter()
        result = con.execute(query).fetchall()
        elapsed = (time.perf_counter() - start) * 1000  # ms
        times.append(elapsed)
        print(f"Run {i}: {elapsed:.2f} ms")
    
    print()
    
    # Print results
    print("Query Results:")
    print("-" * 100)
    print(f"{'returnflag':<12} {'linestatus':<12} {'sum_qty':>15} {'sum_base_price':>18} "
          f"{'sum_disc_price':>18} {'sum_charge':>18} {'avg_qty':>12} {'avg_price':>12} "
          f"{'avg_disc':>10} {'count':>12}")
    print("-" * 100)
    
    for row in result:
        print(f"{row[0]:<12} {row[1]:<12} {row[2]:>15.2f} {row[3]:>18.2f} "
              f"{row[4]:>18.2f} {row[5]:>18.2f} {row[6]:>12.2f} {row[7]:>12.2f} "
              f"{row[8]:>10.2f} {row[9]:>12}")
    
    print()
    
    # Statistics
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0
    min_t = min(times)
    max_t = max(times)
    
    print(f"Performance ({num_runs} runs):")
    print("-" * 40)
    print(f"  Mean:   {mean:.2f} ms")
    print(f"  Stddev: {stdev:.2f} ms")
    print(f"  Min:    {min_t:.2f} ms")
    print(f"  Max:    {max_t:.2f} ms")
    
    con.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_duckdb.py <path_to_lineitem.parquet> [--runs N]")
        sys.exit(1)
    
    data_path = sys.argv[1]
    num_runs = 10
    
    if "--runs" in sys.argv:
        idx = sys.argv.index("--runs")
        num_runs = int(sys.argv[idx + 1])
    
    run_tpch_q1(data_path, num_runs)
