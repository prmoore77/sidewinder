
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
import tarfile
import boto3
import re
from pigz_python import PigzFile


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


def copy_shard_file(src: str, dst: str):
    with Timer(name=f"Copying file: '{src}' to: '{dst}'", text=TIMER_TEXT):
        if re.search(pattern="^s3://", string=dst):
            s3_client = boto3.client('s3')
            bucket_name = dst.split("/")[2]
            dest_file_path = "/".join(dst.split("/")[3:])

            s3_client.upload_file(src, bucket_name, dest_file_path)
        else:
            # Copy the output database file...
            shutil.copy(src=src, dst=dst)


def build_shard(shard_id: int,
                source_data_path: str,
                output_data_path: str,
                overall_shard_count: int,
                duckdb_threads: int,
                duckdb_memory_limit: int,
                working_temporary_dir: str
                ):
    with Timer(name=f"\nBuild Shard ID: {shard_id}", text=TIMER_TEXT):
        shard_name = f"shard_{shard_id}_of_{overall_shard_count}"

        shard_query_list = generate_shard_query_list(data_path=source_data_path,
                                                     shard_count=overall_shard_count,
                                                     shard_id=shard_id
                                                     )

        db_connection = duckdb.connect(database=":memory:")

        db_connection.execute(query=f"PRAGMA threads={duckdb_threads}")
        db_connection.execute(query=f"PRAGMA memory_limit='{duckdb_memory_limit}b'")

        for shard_query in shard_query_list:
            with Timer(name=f"Creating table: {shard_query.table_name} - running SQL: {shard_query.query}", text=TIMER_TEXT):
                db_connection.execute(query=shard_query.query)

        with Timer(name="Running VACUUM ANALYZE", text=TIMER_TEXT):
            db_connection.execute(query="VACUUM ANALYZE")

        with TemporaryDirectory(dir=working_temporary_dir) as output_dir:
            database_directory = os.path.join(output_dir, shard_name)
            with Timer(name=f"Exporting Database to parquet - directory: '{database_directory}'", text=TIMER_TEXT):
                db_connection.execute(f"EXPORT DATABASE '{database_directory}' (FORMAT PARQUET)")

            db_connection.close()

            tarfile_base_name = f"{shard_name}.tar"
            tarfile_name = os.path.join(output_dir, tarfile_base_name)

            with Timer(name=f"Creating tar file: '{tarfile_name}' - from directory: '{database_directory}'", text=TIMER_TEXT):
                with tarfile.open(tarfile_name, "w:") as tar:
                    tar.add(database_directory, arcname=os.path.basename(database_directory))

            with Timer(name=f"Compressing tar file: '{tarfile_name}'", text=TIMER_TEXT):
                pigz_file = PigzFile(compression_target=tarfile_name, workers=duckdb_threads)
                pigz_file.process_compression_target()

            # Copy the output database file...
            copy_shard_file(src=os.path.join(pigz_file.compression_target.parent, pigz_file.output_filename),
                            dst=os.path.join(output_data_path, pigz_file.output_filename)
                            )


@click.command()
@click.option(
    "--shard-count",
    type=int,
    default=os.getenv("SHARD_COUNT"),
    required=True,
    help="How many shards to generate."
)
@click.option(
    "--min-shard",
    type=int,
    default=1,
    required=True,
    help="Minimum shard ID to generate."
)
@click.option(
    "--max-shard",
    type=int,
    default=None,
    help="Maximum shard ID to generate."
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
@click.option(
    "--working-temporary-dir",
    type=str,
    default="/tmp",
    required=True,
    help="The working temporary directory (use nvme for speed)"
)
def main(shard_count: int,
         min_shard: int,
         max_shard: int,
         source_data_path: str,
         output_data_path: str,
         duckdb_threads: int,
         duckdb_memory_limit: int,
         working_temporary_dir: str
         ):

    with Timer(name="\nOverall program", text=TIMER_TEXT):
        logger.info(msg=(f"Running shard generation - (using: "
                         f"--shard-count={shard_count} "
                         f"--min-shard={min_shard} "
                         f"--max-shard={max_shard} "
                         f"--source-data-path='{source_data_path}' "
                         f"--output-data-path='{output_data_path}' "
                         f"--duckdb-threads={duckdb_threads} "
                         f"--duckdb-memory-limit={duckdb_memory_limit} "
                         f"--working-temporary-dir={working_temporary_dir}"
                         )
                    )
        assert max_shard is None or max_shard < shard_count

        for shard_id in range(min_shard, (max_shard or shard_count) + 1):
            build_shard(shard_id=shard_id,
                        source_data_path=source_data_path,
                        output_data_path=output_data_path,
                        overall_shard_count=shard_count,
                        duckdb_threads=duckdb_threads,
                        duckdb_memory_limit=duckdb_memory_limit,
                        working_temporary_dir=working_temporary_dir
                        )


if __name__ == "__main__":
    main()


# Example call:
# python -m build_shard_duckdb --shard-count=101 --min-shard=92 --max-shard=94 --source-data-path="/home/app_user/local_data/tpch_1000" --output-data-path="s3://voltrondata-sidewinder/shards/tpch/1000" --working-temporary-dir="/home/app_user/local_data"
