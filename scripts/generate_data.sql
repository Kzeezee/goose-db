-- Generate TPC-H Parquet files at various scale factors using DuckDB.
-- Usage: duckdb < scripts/generate_data.sql

INSTALL tpch;
LOAD tpch;

-- SF 0.5
CALL dbgen(sf=0.5);
COPY (SELECT * FROM lineitem) TO 'data/sf0.5/lineitem.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM part) TO 'data/sf0.5/part.parquet' (FORMAT PARQUET);
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;

-- SF 1
CALL dbgen(sf=1);
COPY (SELECT * FROM lineitem) TO 'data/sf1/lineitem.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM part) TO 'data/sf1/part.parquet' (FORMAT PARQUET);
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;

-- SF 2
CALL dbgen(sf=2);
COPY (SELECT * FROM lineitem) TO 'data/sf2/lineitem.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM part) TO 'data/sf2/part.parquet' (FORMAT PARQUET);
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;

-- SF 5
CALL dbgen(sf=5);
COPY (SELECT * FROM lineitem) TO 'data/sf5/lineitem.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM part) TO 'data/sf5/part.parquet' (FORMAT PARQUET);
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS supplier;
