import pyarrow
import pyarrow.parquet as pq
import pyarrow.compute as pc
from codetiming import Timer


TPCH_SCALE_FACTOR=10
OVERALL_SHARD_COUNT=10

pyarrow.set_io_thread_count(32)

TIMER_TEXT="{name}: Elapsed time: {:.4f} seconds"


def build_shard(shard_id: int):
    with Timer(name=f"  Build Shard ID: {shard_id}", text=TIMER_TEXT):
        with Timer(name="       Read/Write Region", text=TIMER_TEXT):
            regions = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/region/',
                                    use_threads=True
                                    )
            pq.write_table(regions, "data/tmp/region.parquet")

        with Timer(name="       Read/Write Nation", text=TIMER_TEXT):
            nations = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/nation/',
                                    use_threads=True
                                    )
            pq.write_table(nations, "data/tmp/nation.parquet")

        with Timer(name="       Read Customer Keys", text=TIMER_TEXT):
            filtered_customers = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/customer/',
                                               use_threads=True
                                               )

        custkeys = pc.unique(filtered_customers.column("c_custkey"))

        with Timer(name="       Filter out Cust Keys", text=TIMER_TEXT):
            custkey_list = []
            for custkey in custkeys:
                if (custkey.as_py() % OVERALL_SHARD_COUNT) + 1 == shard_id:
                    custkey_list.append(custkey.as_py())

        with Timer(name="       Read Filtered Orders", text=TIMER_TEXT):
            filtered_orders = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/orders/',
                                            use_threads=True,
                                            filters=[('o_custkey', 'in', custkey_list)]
                                            )

        with Timer(name="       Write out Filtered Orders", text=TIMER_TEXT):
            pq.write_table(filtered_orders, "data/tmp/orders.parquet")

        with Timer(name="       Get Unique Order keys", text=TIMER_TEXT):
            order_keys = pc.unique(filtered_orders.column("o_orderkey"))

        with Timer(name="       Free memory for Filtered Orders", text=TIMER_TEXT):
            del filtered_orders

        with Timer(name="       Read/Filter Line Items", text=TIMER_TEXT):
            filtered_lineitems = pq.read_table(source=f'data/tpch_{TPCH_SCALE_FACTOR}/lineitem/',
                                               use_threads=True,
                                               filters=[('l_orderkey', 'in', order_keys)]
                                               )

        with Timer(name="       Write out Filtered Line Items", text=TIMER_TEXT):
            pq.write_table(filtered_lineitems, "data/tmp/lineitem.parquet")


with Timer(name="Overall program", text=TIMER_TEXT):
    for shard_id in range(1, OVERALL_SHARD_COUNT+1):
        build_shard(shard_id=shard_id)
