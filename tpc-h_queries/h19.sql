SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem,
     part
WHERE (p_partkey = l_partkey
       AND p_brand = 'Brand#12'
       AND p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       AND l_quantity >= 1
       AND l_quantity <= 11
       AND p_size BETWEEN 1 AND 5
       AND l_shipmode in ('AIR',
                          'AIR REG')
       AND l_shipinstruct = 'DELIVER IN PERSON')
  OR (p_partkey = l_partkey
      AND p_brand = 'Brand#23'
      AND p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      AND l_quantity >= 10
      AND l_quantity <= 20
      AND p_size BETWEEN 1 AND 10
      AND l_shipmode in ('AIR',
                         'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON')
  OR (p_partkey = l_partkey
      AND p_brand = 'Brand#34'
      AND p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      AND l_quantity >= 20
      AND l_quantity <= 30
      AND p_size BETWEEN 1 AND 15
      AND l_shipmode in ('AIR',
                         'AIR REG')
      AND l_shipinstruct = 'DELIVER IN PERSON')
;
