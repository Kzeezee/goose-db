"""DuckDB CLI single-threaded baseline benchmark for TPC-H Query 19.

Usage:
    python scripts/duckdb_baseline.py --data data/sf1 --runs 6
    python scripts/duckdb_baseline.py --data data/sf1 --runs 6 --out duckdb_result.csv

Requires: duckdb CLI installed and on PATH.
"""

import argparse
import csv
import re
import subprocess
import sys


def build_sql(data_path):
    return (
        "PRAGMA threads=1;\n"
        "PRAGMA memory_limit='1GB';\n"
        ".timer on\n"
        "SELECT\n"
        "    sum(l_extendedprice * (1 - l_discount)) AS revenue\n"
        "FROM\n"
        f"    '{data_path}/lineitem.parquet' AS lineitem,\n"
        f"    '{data_path}/part.parquet' AS part\n"
        "WHERE\n"
        "    (p_partkey = l_partkey\n"
        "        AND p_brand = 'Brand#12'\n"
        "        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
        "        AND l_quantity >= 1  AND l_quantity <= 1 + 10\n"
        "        AND p_size BETWEEN 1 AND 5\n"
        "        AND l_shipmode IN ('AIR', 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON')\n"
        "    OR (p_partkey = l_partkey\n"
        "        AND p_brand = 'Brand#23'\n"
        "        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
        "        AND l_quantity >= 10 AND l_quantity <= 10 + 10\n"
        "        AND p_size BETWEEN 1 AND 10\n"
        "        AND l_shipmode IN ('AIR', 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON')\n"
        "    OR (p_partkey = l_partkey\n"
        "        AND p_brand = 'Brand#34'\n"
        "        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
        "        AND l_quantity >= 20 AND l_quantity <= 20 + 10\n"
        "        AND p_size BETWEEN 1 AND 15\n"
        "        AND l_shipmode IN ('AIR', 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON');\n"
    )


def build_explain_analyze(data_path):
    return (
        "PRAGMA threads=1;\n"
        "PRAGMA memory_limit='1GB';\n"
        "EXPLAIN ANALYZE\n"
        "SELECT\n"
        "    sum(l_extendedprice * (1 - l_discount)) AS revenue\n"
        "FROM\n"
        f"    '{data_path}/lineitem.parquet' AS lineitem,\n"
        f"    '{data_path}/part.parquet' AS part\n"
        "WHERE\n"
        "    (p_partkey = l_partkey\n"
        "        AND p_brand = 'Brand#12'\n"
        "        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
        "        AND l_quantity >= 1  AND l_quantity <= 1 + 10\n"
        "        AND p_size BETWEEN 1 AND 5\n"
        "        AND l_shipmode IN ('AIR', 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON')\n"
        "    OR (p_partkey = l_partkey\n"
        "        AND p_brand = 'Brand#23'\n"
        "        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
        "        AND l_quantity >= 10 AND l_quantity <= 10 + 10\n"
        "        AND p_size BETWEEN 1 AND 10\n"
        "        AND l_shipmode IN ('AIR', 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON')\n"
        "    OR (p_partkey = l_partkey\n"
        "        AND p_brand = 'Brand#34'\n"
        "        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
        "        AND l_quantity >= 20 AND l_quantity <= 20 + 10\n"
        "        AND p_size BETWEEN 1 AND 15\n"
        "        AND l_shipmode IN ('AIR', 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON');\n"
    )


def run_duckdb(sql):
    """Run SQL via duckdb CLI, return stdout as string."""
    proc = subprocess.run(
        ["duckdb"],
        input=sql.encode("utf-8"),
        capture_output=True,
    )
    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        print(f"duckdb error: {stdout}{stderr}", file=sys.stderr)
        sys.exit(1)
    return stdout


def parse_output(stdout):
    """Parse revenue value and timer from DuckDB box-drawing output.

    stdout contains both the result table and the timer line, e.g.:
        ┌──────────────┐
        │   revenue    │
        │   double     │
        ├──────────────┤
        │ 3083843.0578 │
        └──────────────┘
        Run Time (s): real 0.281 user 0.265625 sys 0.015625
    """
    # Extract timer
    m = re.search(r"real\s+([\d.]+)", stdout)
    if not m:
        print(f"Could not parse timer from output:\n{stdout}", file=sys.stderr)
        sys.exit(1)
    elapsed_s = float(m.group(1))

    # Extract revenue: find a line containing a decimal number inside │ ... │
    revenue = None
    for line in stdout.splitlines():
        # Match lines like │ 3083843.0578 │
        rm = re.search(r"│\s+([\d.]+)\s+│", line)
        if rm:
            try:
                float(rm.group(1))
                revenue = rm.group(1)
            except ValueError:
                pass

    return revenue, elapsed_s


def main():
    parser = argparse.ArgumentParser(description="DuckDB CLI baseline benchmark for TPC-H Q19")
    parser.add_argument("--data", required=True, help="Path to data directory (e.g. data/sf1)")
    parser.add_argument("--runs", type=int, default=6, help="Total runs including warmup (default: 6)")
    parser.add_argument("--out", default="duckdb_result.csv", help="Output CSV path (default: duckdb_result.csv)")
    parser.add_argument("--timing", action="store_true", help="Run EXPLAIN ANALYZE to show per-operator breakdown")
    args = parser.parse_args()

    sql = build_sql(args.data)
    timings = []
    revenue = None

    for run in range(args.runs):
        stdout = run_duckdb(sql)
        rev, elapsed_s = parse_output(stdout)
        elapsed_ms = elapsed_s * 1000

        if rev:
            revenue = rev

        if run == 0:
            print(f"Run {run + 1} (warmup): {elapsed_ms:.2f} ms", file=sys.stderr)
        else:
            print(f"Run {run + 1}: {elapsed_ms:.2f} ms", file=sys.stderr)
            timings.append(elapsed_ms)

    if timings:
        mean_ms = sum(timings) / len(timings)
        print(f"Mean (runs 2-{args.runs}): {mean_ms:.2f} ms", file=sys.stderr)

    if args.timing:
        explain_sql = build_explain_analyze(args.data)
        stdout = run_duckdb(explain_sql)
        print(f"\n--- EXPLAIN ANALYZE ---", file=sys.stderr)
        print(stdout, file=sys.stderr)

    if revenue:
        print(revenue)

        with open(args.out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["revenue"])
            w.writerow([revenue])


if __name__ == "__main__":
    main()
