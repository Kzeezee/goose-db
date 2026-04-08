-- DuckDB single-threaded baseline for TPC-H Query 19.
-- Usage: duckdb < scripts/duckdb_baseline.sql
--
-- Set data path below before running. Writes result to duckdb_result.csv.

PRAGMA threads=1;
PRAGMA memory_limit='1GB';

-- Change this path to match your scale factor
.timer on

WITH lineitem_filtered AS (
    SELECT l_partkey, l_quantity, l_shipmode, l_extendedprice, l_discount
    FROM 'data/sf1/lineitem.parquet' AS lineitem
    WHERE (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
        AND l_quantity <= 30
),
part_filtered AS (
    SELECT p_partkey, p_brand, p_container, p_size
    FROM 'data/sf1/part.parquet' AS part
    WHERE p_brand IN ('Brand#12', 'Brand#23', 'Brand#34')
        AND p_size BETWEEN 1 AND 15
        AND p_container IN (
            'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG',
            'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK',
            'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'
        )
),
lineitem_part_join AS (
    SELECT l.*, p.p_brand, p.p_container, p.p_size
    FROM lineitem_filtered l
    JOIN part_filtered p ON l.l_partkey = p.p_partkey
),
lineitem_part_join_filtered AS (
    SELECT l_extendedprice, l_discount
    FROM lineitem_part_join
    WHERE (p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= 1 AND l_quantity <= 11
            AND p_size BETWEEN 1 AND 5)
        OR (p_brand = 'Brand#23'
            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 20
            AND p_size BETWEEN 1 AND 10)
        OR (p_brand = 'Brand#34'
            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 30
            AND p_size BETWEEN 1 AND 15)
)
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem_part_join_filtered;

.timer off

COPY (
    WITH lineitem_filtered AS (
        SELECT l_partkey, l_quantity, l_shipmode, l_extendedprice, l_discount
        FROM 'data/sf1/lineitem.parquet' AS lineitem
        WHERE (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'
            AND l_quantity <= 30
    ),
    part_filtered AS (
        SELECT p_partkey, p_brand, p_container, p_size
        FROM 'data/sf1/part.parquet' AS part
        WHERE p_brand IN ('Brand#12', 'Brand#23', 'Brand#34')
            AND p_size BETWEEN 1 AND 15
            AND p_container IN (
                'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG',
                'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK',
                'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'
            )
    ),
    lineitem_part_join AS (
        SELECT l.*, p.p_brand, p.p_container, p.p_size
        FROM lineitem_filtered l
        JOIN part_filtered p ON l.l_partkey = p.p_partkey
    ),
    lineitem_part_join_filtered AS (
        SELECT l_extendedprice, l_discount
        FROM lineitem_part_join
        WHERE (p_brand = 'Brand#12'
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 11
                AND p_size BETWEEN 1 AND 5)
            OR (p_brand = 'Brand#23'
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 20
                AND p_size BETWEEN 1 AND 10)
            OR (p_brand = 'Brand#34'
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 30
                AND p_size BETWEEN 1 AND 15)
    )
    SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
    FROM lineitem_part_join_filtered;
) TO 'duckdb_result.csv' (HEADER, DELIMITER ',');
