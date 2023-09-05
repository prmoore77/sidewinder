SELECT n_name
     , p_type
     , sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
     , COUNT(*)                                                  AS count_star
FROM lineitem
         JOIN
     part
     ON lineitem.l_partkey = part.p_partkey
         JOIN
     orders
     ON lineitem.l_orderkey = orders.o_orderkey
         JOIN
     customer
     ON orders.o_custkey = customer.c_custkey
         JOIN
     supplier
     ON lineitem.l_suppkey = supplier.s_suppkey
         JOIN
     nation
     ON supplier.s_nationkey = nation.n_nationkey
         JOIN
     region
     ON nation.n_regionkey = region.r_regionkey
WHERE region.r_name = 'ASIA'
  AND part.p_type = 'ECONOMY ANODIZED STEEL'
  --
  AND orders.o_orderdate >= '1994-01-01'
  AND orders.o_orderdate < '1995-01-01'
GROUP BY n_name
       , p_type
ORDER BY revenue DESC
;
