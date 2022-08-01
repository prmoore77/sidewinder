create table lineitem as select * from read_parquet('1/lineitem/*');
create table customer as select * from read_parquet('1/customer/*');
create table nation as select * from read_parquet('1/nation/*');
create table orders as select * from read_parquet('1/orders/*');
create table part as select * from read_parquet('1/part/*');
create table partsupp as select * from read_parquet('1/partsupp/*');
create table region as select * from read_parquet('1/region/*');
create table supplier as select * from read_parquet('1/supplier/*');
