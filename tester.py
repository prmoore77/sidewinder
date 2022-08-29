import pyarrow
import pyarrow.parquet as pq
import pyarrow.compute as pc

TPCH_SCALE_FACTOR=10
OVERALL_SHARD_COUNT=11
SHARD_ID=1

pyarrow.set_io_thread_count(16)

order_custkeys = pc.unique(pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/orders/',
                                         use_threads=True,
                                         columns=["o_custkey"]
                                         ).column("o_custkey")
                           )

print("read order_custkeys")

order_custkey_list = []
for order_custkey in order_custkeys:
    if (order_custkey.as_py() % OVERALL_SHARD_COUNT) + 1 == SHARD_ID:
        order_custkey_list.append(order_custkey.as_py())

print("filtered out custkeys")

filtered_orders = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/orders/',
                                use_threads=True,
                                filters=[('o_custkey', 'in', order_custkey_list)]
                                )

print("read filtered orders")

# Write out filtered orders...
pq.write_table(filtered_orders, "data/tmp/orders.parquet")

print("wrote out filtered orders")

order_keys = pc.unique(filtered_orders.column("o_orderkey"))

print("got unique order keys")

# Free up some memory
del filtered_orders

print("freed memory for filtered_orders")

filtered_lineitems = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/lineitem/',
                                   use_threads=True,
                                   filters=[('l_orderkey', 'in', order_keys)]
                                   )

print("Filtered line items")

pq.write_table(filtered_lineitems, "data/tmp/lineitem.parquet")

print("wrote out filtered line items")

del filtered_lineitems

print("freed memory for filtered_lineitems")

print('all done')
