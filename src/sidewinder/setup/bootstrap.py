import os
from pathlib import Path

import click

from .build_shards import build_shards
from .create_duckdb_database_from_parquet import create_duckdb_database_from_parquet
from .data_creation_utils import get_printable_number, DATA_DIR, SCRIPT_DIR
from .generate_tpch_parquet_data import generate_tpch_parquet_data
from ..config import logger


# Constants
SEPARATOR = ("=" * 80)


def sidewinder_bootstrap(tpch_scale_factor: float,
                         data_directory: str,
                         shard_count: int,
                         overwrite: bool
                         ):
    logger.info(msg=("Running sidewinder_bootstrap - with parameters: "
                     f"--tpch-scale-factor={tpch_scale_factor} "
                     f"--data-directory='{data_directory}' "
                     f"--shard_count={shard_count} "
                     f"--overwrite={overwrite}"
                     )
                )
    parquet_path = generate_tpch_parquet_data(tpch_scale_factor=tpch_scale_factor,
                                              data_directory=data_directory,
                                              overwrite=overwrite
                                              )

    logger.info(msg=SEPARATOR)
    create_duckdb_database_from_parquet(tpch_scale_factor=tpch_scale_factor,
                                        data_directory=data_directory,
                                        overwrite=overwrite)

    logger.info(msg=SEPARATOR)
    printable_tpch_scale_factor = get_printable_number(tpch_scale_factor)
    build_shards(shard_definition_file=(SCRIPT_DIR / "config" / "tpch_shard_generation_queries.yaml").as_posix(),
                 shard_count=shard_count,
                 source_data_path=parquet_path.as_posix(),
                 output_data_path=(Path(data_directory) / "shards" / "tpch" / printable_tpch_scale_factor).as_posix(),
                 overwrite=overwrite
                 )

    logger.info(msg=SEPARATOR)
    logger.info(msg="All bootstrap steps completed")


@click.command()
@click.option(
    "--tpch-scale-factor",
    type=float,
    default=1,
    show_default=True,
    required=True,
    help="The TPC-H scale factor to generate."
)
@click.option(
    "--data-directory",
    type=str,
    default=DATA_DIR.as_posix(),
    show_default=True,
    required=True,
    help="The main data directory"
)
@click.option(
    "--shard-count",
    type=int,
    default=11,
    show_default=True,
    required=True,
    help="The number of shards to create"
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target database file if it already exists..."
)
def main(tpch_scale_factor: float,
         data_directory: str,
         shard_count: int,
         overwrite: bool):
    sidewinder_bootstrap(**locals())


if __name__ == "__main__":
    main()
