-- DuckDB single-threaded baseline for TPC-H Query 19.
-- Usage: duckdb < scripts/duckdb_baseline.sql
--
-- Set data path below before running. Writes result to duckdb_result.csv.

PRAGMA threads=1;
PRAGMA memory_limit='1GB';

-- Change this path to match your scale factor
.timer on

SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    'data/sf1/lineitem.parquet' AS lineitem,
    'data/sf1/part.parquet' AS part
WHERE
    (p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1  AND l_quantity <= 1 + 10
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON');

.timer off

COPY (
    SELECT
        sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM
        'data/sf1/lineitem.parquet' AS lineitem,
        'data/sf1/part.parquet' AS part
    WHERE
        (p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= 1  AND l_quantity <= 1 + 10
            AND p_size BETWEEN 1 AND 5
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON')
        OR (p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 10 + 10
            AND p_size BETWEEN 1 AND 10
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON')
        OR (p_partkey = l_partkey
            AND p_brand = 'Brand#34'
            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 20 + 10
            AND p_size BETWEEN 1 AND 15
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON')
) TO 'duckdb_result.csv' (HEADER, DELIMITER ',');
