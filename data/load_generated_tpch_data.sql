-- First:
-- git clone https://github.com/electrum/tpch-dbgen.git
-- cd tpch-dbgen
-- make
-- ./dbgen -v -s 10

-- Start DuckDB and run the following SQL
CREATE TABLE lineitem(l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber BIGINT, l_quantity FLOAT, l_extendedprice FLOAT, l_discount FLOAT, l_tax FLOAT, l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR);
CREATE TABLE customer(c_custkey BIGINT, c_name VARCHAR, c_address VARCHAR, c_nationkey BIGINT, c_phone VARCHAR, c_acctbal FLOAT, c_mktsegment VARCHAR, c_comment VARCHAR);
CREATE TABLE nation(n_nationkey BIGINT, n_name VARCHAR, n_regionkey BIGINT, n_comment VARCHAR);
CREATE TABLE orders(o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus VARCHAR, o_totalprice FLOAT, o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority BIGINT, o_comment VARCHAR);
CREATE TABLE part(p_partkey BIGINT, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size BIGINT, p_container VARCHAR, p_retailprice FLOAT, p_comment VARCHAR);
CREATE TABLE partsupp(ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty BIGINT, ps_supplycost FLOAT, ps_comment VARCHAR);
CREATE TABLE region(r_regionkey BIGINT, r_name VARCHAR, r_comment VARCHAR);
CREATE TABLE supplier(s_suppkey BIGINT, s_name VARCHAR, s_address VARCHAR, s_nationkey BIGINT, s_phone VARCHAR, s_acctbal FLOAT, s_comment VARCHAR);

INSERT INTO region (r_regionkey, r_name, r_comment)
SELECT column0
     , column1
     , column2
  FROM read_csv_auto('/tmp/tpch-dbgen/region.tbl', delim='|', header=False);

INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)
SELECT column0
     , column1
     , column2
     , column3
  FROM read_csv_auto('/tmp/tpch-dbgen/nation.tbl', delim='|', header=False);

INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
SELECT column0
     , column1
     , column2
     , column3
     , column4
     , column5
     , column6
  FROM read_csv_auto('/tmp/tpch-dbgen/supplier.tbl', delim='|', header=False);

INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
SELECT column0
     , column1
     , column2
     , column3
     , column4
     , column5
     , column6
     , column7
     , column8
  FROM read_csv_auto('/tmp/tpch-dbgen/part.tbl', delim='|', header=False);

INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
SELECT column0
     , column1
     , column2
     , column3
     , column4
  FROM read_csv_auto('/tmp/tpch-dbgen/partsupp.tbl', delim='|', header=False);

INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
SELECT column0
     , column1
     , column2
     , column3
     , column4
     , column5
     , column6
     , column7
  FROM read_csv_auto('/tmp/tpch-dbgen/customer.tbl', delim='|', header=False);

INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk,
                    o_shippriority, o_comment)
SELECT column0
     , column1
     , column2
     , column3
     , column4
     , column5
     , column6
     , column7
     , column8
  FROM read_csv_auto('/tmp/tpch-dbgen/orders.tbl', delim='|', header=False);

INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
                      l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
                      l_comment)
SELECT column00
     , column01
     , column02
     , column03
     , column04
     , column05
     , column06
     , column07
     , column08
     , column09
     , column10
     , column11
     , column12
     , column13
     , column14
     , column15
  FROM read_csv_auto('/tmp/tpch-dbgen/lineitem.tbl', delim='|', header=False);

VACUUM ANALYZE;

EXPORT DATABASE './tpch10_export' (FORMAT PARQUET);
