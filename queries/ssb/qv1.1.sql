WITH customer_trip_aggregation AS (
    SELECT customer.c_nation
         , part.p_category
         , dates.d_year
         --
         , customer.c_custkey /* Needed b/c we are calculating trips per customer to determine repeat customers */
         --
         , SUM (lineorder.lo_quantity) AS sum_quantity
         , SUM (lineorder.lo_extendedprice) AS sum_sales
         , COUNT (DISTINCT lineorder.lo_orderkey) AS count_distinct_orders
         , COUNT (*) AS count_star
      FROM lineorder
        JOIN customer
      ON lineorder.lo_custkey = customer.c_custkey
        JOIN part
      ON lineorder.lo_partkey = part.p_partkey
        JOIN date_dim dates
      ON lineorder.lo_orderdate = dates.d_datekey
      GROUP BY
           customer.c_nation
         , part.p_category
         , dates.d_year
         --
         , customer.c_custkey
      )
/* Now aggregate the data the rest of the way, removing Customer from GROUP BY */
SELECT c_nation
     , p_category
     , d_year
     --
     , SUM (sum_quantity) AS sum_quantity
     , SUM (sum_sales) AS sum_sales
     , SUM (count_distinct_orders) AS count_distinct_orders /* This is safe b/c an order can only belong to one customer */
     , COUNT (/* DISTINCT */ c_custkey) AS count_distinct_customers /* This is safe, b/c we grouped by all keys + the customer key in the previous query block */
     , COUNT (CASE WHEN count_distinct_orders > 1 THEN c_custkey END) AS count_distinct_repeat_customers
     , SUM (count_star) AS count_star
  FROM customer_trip_aggregation
GROUP BY c_nation
     , p_category
     , d_year
ORDER BY c_nation
     , p_category
     , d_year
;
