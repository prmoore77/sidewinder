create table lineitem as select * from parquet_scan('10/lineitem/*');
create table customer as select * from parquet_scan('10/customer/*');
create table nation as select * from parquet_scan('10/nation/*');
create table orders as select * from parquet_scan('10/orders/*');
create table part as select * from parquet_scan('10/part/*');
create table partsupp as select * from parquet_scan('10/partsupp/*');
create table region as select * from parquet_scan('10/region/*');
create table supplier as select * from parquet_scan('10/supplier/*');
