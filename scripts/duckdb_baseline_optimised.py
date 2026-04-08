"""DuckDB CLI single-threaded baseline benchmark for TPC-H Query 19 (optimised pushdown).

Usage:
    python scripts/duckdb_baseline_optimised.py --data data/sf1 --runs 6
    python scripts/duckdb_baseline_optimised.py --data data/sf1 --runs 6 --out duckdb_result_opt.csv

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
        "WITH lineitem_filtered AS (\n"
        "    SELECT l_partkey, l_quantity, l_shipmode, l_extendedprice, l_discount\n"
        f"    FROM '{data_path}/lineitem.parquet' AS lineitem\n"
        "    WHERE (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON'\n"
        "        AND l_quantity <= 30\n"
        "),\n"
        "part_filtered AS (\n"
        "    SELECT p_partkey, p_brand, p_container, p_size\n"
        f"    FROM '{data_path}/part.parquet' AS part\n"
        "    WHERE p_brand IN ('Brand#12', 'Brand#23', 'Brand#34')\n"
        "        AND p_size BETWEEN 1 AND 15\n"
        "        AND p_container IN (\n"
        "            'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG',\n"
        "            'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK',\n"
        "            'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'\n"
        "        )\n"
        "),\n"
        "lineitem_part_join AS (\n"
        "    SELECT l.*, p.p_brand, p.p_container, p.p_size\n"
        "    FROM lineitem_filtered l\n"
        "    JOIN part_filtered p ON l.l_partkey = p.p_partkey\n"
        "),\n"
        "lineitem_part_join_filtered AS (\n"
        "    SELECT l_extendedprice, l_discount\n"
        "    FROM lineitem_part_join\n"
        "    WHERE (p_brand = 'Brand#12'\n"
        "            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
        "            AND l_quantity >= 1 AND l_quantity <= 11\n"
        "            AND p_size BETWEEN 1 AND 5)\n"
        "        OR (p_brand = 'Brand#23'\n"
        "            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
        "            AND l_quantity >= 10 AND l_quantity <= 20\n"
        "            AND p_size BETWEEN 1 AND 10)\n"
        "        OR (p_brand = 'Brand#34'\n"
        "            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
        "            AND l_quantity >= 20 AND l_quantity <= 30\n"
        "            AND p_size BETWEEN 1 AND 15)\n"
        ")\n"
        "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue\n"
        "FROM lineitem_part_join_filtered;\n"
    )


def build_explain_analyze(data_path):
    return (
        "PRAGMA threads=1;\n"
        "PRAGMA memory_limit='1GB';\n"
        "EXPLAIN ANALYZE\n"
        "WITH lineitem_filtered AS (\n"
        "    SELECT l_partkey, l_quantity, l_shipmode, l_extendedprice, l_discount\n"
        f"    FROM '{data_path}/lineitem.parquet' AS lineitem\n"
        "    WHERE (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')\n"
        "        AND l_shipinstruct = 'DELIVER IN PERSON'\n"
        "        AND l_quantity <= 30\n"
        "),\n"
        "part_filtered AS (\n"
        "    SELECT p_partkey, p_brand, p_container, p_size\n"
        f"    FROM '{data_path}/part.parquet' AS part\n"
        "    WHERE p_brand IN ('Brand#12', 'Brand#23', 'Brand#34')\n"
        "        AND p_size BETWEEN 1 AND 15\n"
        "        AND p_container IN (\n"
        "            'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG',\n"
        "            'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK',\n"
        "            'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'\n"
        "        )\n"
        "),\n"
        "lineitem_part_join AS (\n"
        "    SELECT l.*, p.p_brand, p.p_container, p.p_size\n"
        "    FROM lineitem_filtered l\n"
        "    JOIN part_filtered p ON l.l_partkey = p.p_partkey\n"
        "),\n"
        "lineitem_part_join_filtered AS (\n"
        "    SELECT l_extendedprice, l_discount\n"
        "    FROM lineitem_part_join\n"
        "    WHERE (p_brand = 'Brand#12'\n"
        "            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
        "            AND l_quantity >= 1 AND l_quantity <= 11\n"
        "            AND p_size BETWEEN 1 AND 5)\n"
        "        OR (p_brand = 'Brand#23'\n"
        "            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
        "            AND l_quantity >= 10 AND l_quantity <= 20\n"
        "            AND p_size BETWEEN 1 AND 10)\n"
        "        OR (p_brand = 'Brand#34'\n"
        "            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
        "            AND l_quantity >= 20 AND l_quantity <= 30\n"
        "            AND p_size BETWEEN 1 AND 15)\n"
        ")\n"
        "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue\n"
        "FROM lineitem_part_join_filtered;\n"
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
    parser = argparse.ArgumentParser(description="DuckDB CLI optimised baseline benchmark for TPC-H Q19")
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
