import os
import pathlib
import re
import shutil
import tarfile
import uuid
from tempfile import TemporaryDirectory

import boto3
import click
import duckdb
import yaml
import zstandard
from codetiming import Timer
from munch import munchify, Munch
from datetime import datetime, UTC
import hashlib

from .data_creation_utils import SCRIPT_DIR
from ..config import logger, DATA_DIR
from ..utils import get_cpu_count, get_memory_limit, get_sha256_hash, get_md5_hash

# Constants
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"
DEFAULT_ZSTD_COMPRESSION_LEVEL = 3


def generate_shard_query_list(shard_tables: Munch,
                              data_path: str,
                              shard_count: int,
                              shard_number: int,
                              parent_table_name: str = "") -> []:
    shard_query_list = []
    for table in shard_tables.tables:
        if table.parent_table_name == parent_table_name:
            format_dict = dict(overall_shard_count=shard_count,
                               shard_number=shard_number,
                               parent_table_dataset=parent_table_name,
                               data_path=data_path
                               )
            table_generation_query = f"CREATE TABLE {table.name} AS {table.query_template.format(**format_dict)}"
            shard_query_list.append(Munch(table_name=table.name, query=table_generation_query))

            # Call the routine recursively to get the tree of tables...
            shard_query_list += generate_shard_query_list(shard_tables, data_path, shard_count, shard_number,
                                                          table.name)

    return shard_query_list


def copy_shard_file(src: str, dst: str):
    with Timer(name=f"Copying file: '{src}' to: '{dst}'", text=TIMER_TEXT):
        if re.search(pattern=r"^s3://", string=dst):
            s3_client = boto3.client('s3')
            bucket_name = dst.split("/")[2]
            dest_file_path = "/".join(dst.split("/")[3:])

            s3_client.upload_file(src, bucket_name, dest_file_path)
        else:
            # Copy the output database file...
            pathlib.Path(dst).parent.resolve().mkdir(parents=True, exist_ok=True)
            shutil.copy(src=src, dst=dst)


def build_shard(shard_tables: Munch,
                shard_number: int,
                source_data_path: str,
                output_data_path: str,
                overall_shard_count: int,
                zstd_compression_level: int,
                duckdb_threads: int,
                duckdb_memory_limit: int,
                working_temporary_dir: str
                ) -> Munch:
    with (Timer(name=f"\nBuild Shard ID: {shard_number}", text=TIMER_TEXT)):
        start_timestamp = datetime.now(tz=UTC)
        shard_name = f"shard_{shard_number}_of_{overall_shard_count}"

        shard_query_list = generate_shard_query_list(shard_tables=shard_tables,
                                                     data_path=source_data_path,
                                                     shard_count=overall_shard_count,
                                                     shard_number=shard_number
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
            tarfile_path = pathlib.Path(output_dir) / tarfile_base_name
            zstd_file_path = pathlib.Path(f"{tarfile_path.as_posix()}.zst")

            with Timer(name=f"Creating compressed tar file: '{zstd_file_path.as_posix()}", text=TIMER_TEXT):
                cctx = zstandard.ZstdCompressor(level=zstd_compression_level, threads=duckdb_threads)
                with zstandard.open(zstd_file_path.absolute(), "wb", cctx=cctx) as zstd_file:
                    with tarfile.open(fileobj=zstd_file, mode="w") as tar:
                        tar.add(database_directory, arcname=os.path.basename(database_directory))

                zstd_file_size = os.path.getsize(filename=zstd_file_path)

            # Get a SHA256 Hash of the ZSTD file...
            with Timer(name=f"Calculating SHA256 hash of: '{zstd_file_path.as_posix()}'", text=TIMER_TEXT):
                sha256_hash = get_sha256_hash(file_path=zstd_file_path)

            with Timer(name=f"Calculating MD5 hash of: '{zstd_file_path.as_posix()}'", text=TIMER_TEXT):
                md5_hash = get_md5_hash(file_path=zstd_file_path)

            shard_file_name = os.path.join(output_data_path, zstd_file_path.name)

            # Copy the output database file...
            copy_shard_file(src=zstd_file_path.as_posix(),
                            dst=shard_file_name
                            )

            return Munch(shard_id=str(uuid.uuid4()),
                         shard_number=shard_number,
                         shard_name=shard_name,
                         shard_file_name=shard_file_name,
                         shard_file_size=zstd_file_size,
                         shard_file_sha256_hash=sha256_hash,
                         shard_file_md5_hash=md5_hash,
                         tarfile_path=tarfile_path.name,
                         build_start_timestamp=start_timestamp.isoformat(),
                         build_end_timestamp=datetime.now(tz=UTC).isoformat()
                         )


def build_shards(shard_definition_file: str,
                 shard_manifest_file: str,
                 shard_count: int,
                 source_data_path: str,
                 output_data_path: str,
                 min_shard: int = 1,
                 max_shard: int = None,
                 zstd_compression_level: int = DEFAULT_ZSTD_COMPRESSION_LEVEL,
                 duckdb_threads: int = get_cpu_count(),
                 duckdb_memory_limit: int = get_memory_limit(),
                 working_temporary_dir: str = "/tmp",
                 overwrite: bool = False
                 ):
    with Timer(name="\nOverall program", text=TIMER_TEXT):
        logger.info(msg=(f"Running shard generation - (using: "
                         f"--shard-definition-file='{shard_definition_file}' "
                         f"--shard-manifest-file='{shard_manifest_file}' "
                         f"--shard-count={shard_count} "
                         f"--min-shard={min_shard} "
                         f"--max-shard={max_shard} "
                         f"--source-data-path='{source_data_path}' "
                         f"--output-data-path='{output_data_path}' "
                         f"--zstd-compression-level={zstd_compression_level} "
                         f"--duckdb-threads={duckdb_threads} "
                         f"--duckdb-memory-limit={duckdb_memory_limit} "
                         f"--working-temporary-dir={working_temporary_dir} "
                         f"--overwrite={overwrite} "
                         )
                    )
        assert max_shard is None or max_shard <= shard_count

        overall_start_timestamp = datetime.now(tz=UTC)

        # Read our source table shard generation info
        with open(shard_definition_file, "r") as data:
            shard_tables = munchify(x=yaml.safe_load(data.read()))

        shard_manifest_file_path = pathlib.Path(shard_manifest_file)
        if shard_manifest_file_path.exists():
            if overwrite:
                logger.warning(msg=f"File: {shard_manifest_file_path.as_posix()} exists, removing...")
                shard_manifest_file_path.unlink()
            else:
                raise RuntimeError(f"File: {shard_manifest_file_path.as_posix()} exists, aborting.")

        target_directory = pathlib.Path(output_data_path)
        if target_directory.exists():
            if overwrite:
                logger.warning(msg=f"Directory: {target_directory.as_posix()} exists, removing...")
                shutil.rmtree(path=target_directory.as_posix())
            else:
                raise RuntimeError(f"Directory: {target_directory.as_posix()} exists, aborting.")

        shard_manifest_dict_list = []
        for shard_number in range(min_shard, (max_shard or shard_count) + 1):
            shard_munch = build_shard(shard_tables=shard_tables,
                                      shard_number=shard_number,
                                      source_data_path=source_data_path,
                                      output_data_path=output_data_path,
                                      overall_shard_count=shard_count,
                                      zstd_compression_level=zstd_compression_level,
                                      duckdb_threads=duckdb_threads,
                                      duckdb_memory_limit=duckdb_memory_limit,
                                      working_temporary_dir=working_temporary_dir
                                      )
            shard_manifest_dict_list.append(shard_munch.toDict())

        # Create directory for the shard manifest file if needed...
        shard_manifest_file_path = pathlib.Path(shard_manifest_file)
        shard_manifest_file_path.parent.resolve().mkdir(parents=True, exist_ok=True)

        with open(shard_manifest_file_path, "w") as data:
            data.write(yaml.safe_dump(data=dict(shard_count=shard_count,
                                                source_data_path=source_data_path,
                                                output_data_path=output_data_path,
                                                zstd_compression_level=zstd_compression_level,
                                                overall_start_timestamp=overall_start_timestamp.isoformat(),
                                                overall_end_timestamp=datetime.now(tz=UTC).isoformat(),
                                                duckdb_version=duckdb.__version__,
                                                shard_list=shard_manifest_dict_list
                                                ),
                                      default_flow_style=False,
                                      sort_keys=False,
                                      indent=2
                                      )
                       )


@click.command()
@click.option(
    "--shard-definition-file",
    type=str,
    default=(SCRIPT_DIR / "config" / "tpch_shard_generation_queries.yaml").as_posix(),
    required=True,
    show_default=True,
    help="The file that contains the tables for the shard data model, and the queries used to create them for the shard."
)
@click.option(
    "--shard-manifest-file",
    type=str,
    default=(DATA_DIR / "shards" / "manifests" / "local_tpch_sf1_shard_manifest.yaml").as_posix(),
    required=True,
    show_default=True,
    help="The output file path will have details about the shards created by this process."
)
@click.option(
    "--shard-count",
    type=int,
    default=os.getenv("SHARD_COUNT"),
    show_default=True,
    required=True,
    help="How many shards to generate."
)
@click.option(
    "--min-shard",
    type=int,
    default=1,
    show_default=True,
    required=True,
    help="Minimum shard ID to generate."
)
@click.option(
    "--max-shard",
    type=int,
    default=None,
    show_default=True,
    help="Maximum shard ID to generate."
)
@click.option(
    "--source-data-path",
    type=str,
    default=os.getenv("SOURCE_DATA_PATH", (DATA_DIR / "tpch" / "sf=1").as_posix()),
    show_default=True,
    required=True,
    help="The source parquet data path"
)
@click.option(
    "--output-data-path",
    type=str,
    default=os.getenv("OUTPUT_DATA_PATH", (DATA_DIR / "shards" / "tpch" / "sf=1").as_posix()),
    show_default=True,
    required=True,
    help="The target database output path"
)
@click.option(
    "--zstd-compression-level",
    type=int,
    default=DEFAULT_ZSTD_COMPRESSION_LEVEL,
    show_default=True,
    help="The ZStandard compression level to use when compressing the shard tar files."
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
    show_default=True,
    required=True,
    help="The working temporary directory (use nvme for speed)"
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target shard directory if it already exists..."
)
def main(shard_definition_file: str,
         shard_manifest_file: str,
         shard_count: int,
         min_shard: int,
         max_shard: int,
         source_data_path: str,
         output_data_path: str,
         zstd_compression_level: int,
         duckdb_threads: int,
         duckdb_memory_limit: int,
         working_temporary_dir: str,
         overwrite: bool):
    build_shards(**locals())


if __name__ == "__main__":
    main()

# Example call:
# python -m build_shard_duckdb --shard-count=1001 --min-shard=901 --max-shard=1001 --source-data-path="/home/app_user/data/tpch_10000"
# --output-data-path="s3://voltrondata-sidewinder/shards/tpch/10000" --working-temporary-dir="/home/app_user/data"
