from ..utils import logger
import duckdb
import click
from pathlib import Path
from .data_creation_utils import execute_query, get_printable_number, pushd
from sidewinder.config import DATA_DIR


def create_duckdb_database_from_parquet(benchmark_name: str,
                                        scale_factor: int,
                                        data_directory: str,
                                        overwrite: bool
                                        ):
    logger.info(msg=(f"Creating a DuckDB with parameters: "
                     f"--benchmark-name={benchmark_name} "
                     f"--scale-factor={scale_factor} "
                     f"--data-directory={data_directory} "
                     f"--overwrite={overwrite}"
                     )
                )

    # Output the database version
    logger.info(msg=f"Using DuckDB Version: {duckdb.__version__}")

    print_scale_factor = get_printable_number(scale_factor)
    database_file = Path(data_directory) / f"{benchmark_name}_sf{print_scale_factor}.duckdb"

    # Delete the file if it exists...
    if database_file.exists():
        if overwrite:
            logger.warning(msg=f"Database file: {database_file.as_posix()} already exists, removing...")
            database_file.unlink()
        else:
            raise RuntimeError(f"Database file: {database_file.as_posix()} already exists, aborting.")

    logger.info(msg=f"Creating database file: {database_file.as_posix()}")

    # Get an in-memory DuckDB database connection
    conn = duckdb.connect(database=database_file.as_posix())

    data_path = Path(data_directory)
    working_dir = data_path.parent
    parquet_file_path = data_path / benchmark_name / f"sf={print_scale_factor}"

    with pushd(working_dir):
        for path_object in parquet_file_path.glob(pattern="*"):
            if path_object.is_dir():
                table_name = path_object.name
                table_parquet_path = parquet_file_path / table_name
                if table_name == "date":
                    table_name = "date_dim"
                sql_statement = (f"CREATE OR REPLACE VIEW {table_name} AS "
                                 f"SELECT * FROM read_parquet('{table_parquet_path.relative_to(working_dir)}/*.parquet');"
                                 )
                execute_query(conn=conn, query=sql_statement)

    logger.info(msg="All done.")


@click.command()
@click.option(
    "--benchmark-name",
    type=str,
    default="tpch",
    show_default=True,
    required=True,
    help="The benchmark name (used to determine the path and tables)."
)
@click.option(
    "--scale-factor",
    type=float,
    default=1,
    show_default=True,
    required=True,
    help="The benchmark scale factor to generate."
)
@click.option(
    "--data-directory",
    type=str,
    default=DATA_DIR.as_posix(),
    show_default=True,
    required=True,
    help=("The data directory containing the parquet files (will look for a sub-directory "
          f"with name: {DATA_DIR.as_posix()}/{{tpch-scale-factor}}.  "
          f"This directory will also be used as the target for creating the DuckDB database file."
          )
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target database file if it already exists..."
)
def main(benchmark_name: str,
         scale_factor: int,
         data_directory: str,
         overwrite: bool):
    create_duckdb_database_from_parquet(**locals())


if __name__ == "__main__":
    main()
