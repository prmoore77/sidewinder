from codetiming import Timer
from munch import munchify, Munch
from tempfile import TemporaryDirectory
import duckdb
import yaml
import click
import os
from utils import get_cpu_count, get_memory_limit
from config import logger
import shutil

TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"

# Read our source table shard generation info
with open("shard_generation_queries.yaml", "r") as data:
    TABLES = munchify(x=yaml.safe_load(data.read()))


def generate_shard_query_list(data_path: str,
                              shard_count: int,
                              shard_id: int,
                              parent_table_name: str = "") -> []:
    shard_query_list = []
    for table in TABLES.tables:
        if table.parent_table_name == parent_table_name:
            format_dict = dict(overall_shard_count=shard_count,
                               shard_id=shard_id,
                               parent_table_dataset=parent_table_name,
                               data_path=data_path
                               )
            table_generation_query = f"CREATE TABLE {table.name} AS {table.query_template.format(**format_dict)}"
            shard_query_list.append(Munch(table_name=table.name, query=table_generation_query))

            # Call the routine recursively to get the tree of tables...
            shard_query_list += generate_shard_query_list(data_path, shard_count, shard_id,
                                                          table.name)

    return shard_query_list


def build_shard(shard_id: int,
                source_data_path: str,
                output_data_path: str,
                overall_shard_count: int,
                duckdb_threads: int,
                duckdb_memory_limit: int
                ):
    with Timer(name=f"\nBuild Shard ID: {shard_id}", text=TIMER_TEXT):
        shard_query_list = generate_shard_query_list(data_path=source_data_path,
                                                     shard_count=overall_shard_count,
                                                     shard_id=shard_id
                                                     )

        with TemporaryDirectory(dir=output_data_path) as output_dir:
            database_file = os.path.join(output_dir, f"shard_{shard_id}_of_{overall_shard_count}.db")
            logger.info(msg=f"Creating new database file: {database_file}")

            db_connection = duckdb.connect(database=database_file,
                                           read_only=False
                                           )
            db_connection.execute(query=f"PRAGMA threads={duckdb_threads}")
            db_connection.execute(query=f"PRAGMA memory_limit='{duckdb_memory_limit}b'")

            for shard_query in shard_query_list:
                with Timer(name=f"Creating table: {shard_query.table_name} - running SQL: {shard_query.query}", text=TIMER_TEXT):
                    db_connection.execute(query=shard_query.query)

            with Timer(name="Running VACUUM ANALYZE", text=TIMER_TEXT):
                db_connection.execute(query="VACUUM ANALYZE")

            db_connection.close()

            # Copy the output database file...
            shutil.copy(src=database_file, dst=output_data_path)


@click.command()
@click.option(
    "--shard-count",
    type=int,
    default=os.getenv("SHARD_COUNT"),
    required=True,
    help="How many shards to generate."
)
@click.option(
    "--source-data-path",
    type=str,
    default=os.getenv("SOURCE_DATA_PATH"),
    required=True,
    help="The source parquet data path"
)
@click.option(
    "--output-data-path",
    type=str,
    default=os.getenv("OUTPUT_DATA_PATH"),
    required=True,
    help="The target database output path"
)
@click.option(
    "--duckdb-threads",
    type=int,
    default=os.getenv("DUCKDB_THREADS", get_cpu_count()),
    show_default=True,
    help="The number of DuckDB threads to use - default is to use all CPU threads available."
)
@click.option(
    "--duckdb-memory-limit",
    type=int,
    default=os.getenv("DUCKDB_MEMORY_LIMIT", int(0.75 * float(get_memory_limit()))),
    show_default=True,
    help="The amount of memory to allocate to DuckDB - default is to use 75% of physical memory available."
)
def main(shard_count: int,
         source_data_path: str,
         output_data_path: str,
         duckdb_threads: int,
         duckdb_memory_limit: int
         ):
    with Timer(name="\nOverall program", text=TIMER_TEXT):
        logger.info(msg=(f"Running shard generation - (using: "
                         f"--shard-count={shard_count} "
                         f"--source-data-path='{source_data_path}' "
                         f"--output-data-path='{output_data_path}' "
                         f"--duckdb-threads={duckdb_threads} "
                         f"--duckdb-memory-limit={duckdb_memory_limit}"
                         )
                    )
        for shard_id in range(1, shard_count + 1):
            build_shard(shard_id=shard_id,
                        source_data_path=source_data_path,
                        output_data_path=output_data_path,
                        overall_shard_count=shard_count,
                        duckdb_threads=duckdb_threads,
                        duckdb_memory_limit=duckdb_memory_limit
                        )


if __name__ == "__main__":
    main()
