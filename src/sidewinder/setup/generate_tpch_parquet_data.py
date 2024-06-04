from ..utils import logger
import duckdb
import click
from pathlib import Path
import shutil
from .data_creation_utils import execute_query, get_printable_number
from sidewinder.config import DATA_DIR


def generate_tpch_parquet_data(tpch_scale_factor: int,
                               data_directory: str,
                               overwrite: bool
                               ) -> Path:
    logger.info(msg=("Creating a TPC-H parquet dataset - with parameters: "
                     f"--tpch-scale-factor={tpch_scale_factor} "
                     f"--data-directory='{data_directory}' "
                     f"--overwrite={overwrite}"
                     )
                )

    # Output the database version
    logger.info(msg=f"Using DuckDB Version: {duckdb.__version__}")

    # Get an in-memory DuckDB database connection
    conn = duckdb.connect()

    # Load the TPCH extension needed to generate the data...
    conn.load_extension(extension="tpch")

    # Generate the data
    execute_query(conn=conn, query=f"CALL dbgen(sf={tpch_scale_factor})")

    # Export the data
    target_directory = Path(f"{data_directory}/tpch/sf={get_printable_number(tpch_scale_factor)}")

    if target_directory.exists():
        if overwrite:
            logger.warning(msg=f"Directory: {target_directory.as_posix()} exists, removing...")
            shutil.rmtree(path=target_directory.as_posix())
        else:
            raise RuntimeError(f"Directory: {target_directory.as_posix()} exists, aborting.")

    target_directory.mkdir(parents=True, exist_ok=True)
    execute_query(conn=conn, query=f"EXPORT DATABASE '{target_directory.as_posix()}' (FORMAT PARQUET)")

    logger.info(msg=f"Wrote out parquet data to path: '{target_directory.as_posix()}'")

    # Restructure the contents of the directory so that each file is in its own directory
    for filename in target_directory.glob(pattern="*.parquet"):
        file = Path(filename)
        table_name = file.name.split(".")[0]
        table_directory = target_directory / table_name
        table_directory.mkdir(parents=True, exist_ok=True)

        if file.name not in ("nation.parquet", "region.parquet"):
            new_file_name = f"{table_name}.1.parquet"
        else:
            new_file_name = file.name

        file.rename(target=table_directory / new_file_name)

    logger.info(msg="All done.")

    return target_directory


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
    help="The target output data directory to put the files into"
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target directory if it already exists..."
)
def main(tpch_scale_factor: int,
         data_directory: str,
         overwrite: bool
         ):
    generate_tpch_parquet_data(**locals())


if __name__ == "__main__":
    main()
