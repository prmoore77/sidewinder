SELECT p_brand,
       p_type,
       p_size,
       count(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp,
     part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type not like 'MEDIUM POLISHED%'
  AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey not in
    (SELECT s_suppkey
     FROM supplier
     WHERE s_comment like '%Customer%Complaints%' )
GROUP BY p_brand,
         p_type,
         p_size
ORDER BY supplier_cnt DESC,
         p_brand,
         p_type,
         p_size
;

CREATE TABLE orders AS SELECT CAST (1 AS INTEGER) AS shard_id
, orders.*
FROM read_parquet('./tpch_1000/orders/orders.*.parquet') AS orders
WHERE mod (abs (hash (o_custkey)), 111) + 1 = 1;
select count(*) from orders;


CREATE TEMPORARY TABLE lineitem AS
SELECT *
FROM read_parquet('./tpch_1000/lineitem/lineitem.*.parquet')
WHERE l_orderkey IN (SELECT o_orderkey
FROM orders
);
