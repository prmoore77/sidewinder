create table lineitem as select * from parquet_scan('1/lineitem/*');
create table customer as select * from parquet_scan('1/customer/*');
create table nation as select * from parquet_scan('1/nation/*');
create table orders as select * from parquet_scan('1/orders/*');
create table part as select * from parquet_scan('1/part/*');
create table partsupp as select * from parquet_scan('1/partsupp/*');
create table region as select * from parquet_scan('1/region/*');
create table supplier as select * from parquet_scan('1/supplier/*');
