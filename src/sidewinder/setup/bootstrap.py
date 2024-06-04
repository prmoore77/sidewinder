import os
from pathlib import Path

import click

from .build_shards import build_shards
from .create_duckdb_database_from_parquet import create_duckdb_database_from_parquet
from .data_creation_utils import get_printable_number, SCRIPT_DIR
from .generate_tpch_parquet_data import generate_tpch_parquet_data
from ..config import logger, DATA_DIR
from ..security import create_user, SECRET_KEY
from ..constants import USER_LIST_FILENAME
from .tls_utilities import create_tls_keypair, DEFAULT_CERT_FILE, DEFAULT_KEY_FILE


# Constants
SEPARATOR = ("=" * 80)


@click.command()
@click.option(
    "--tls-cert-file",
    type=str,
    default=DEFAULT_CERT_FILE,
    required=True,
    help="The TLS certificate file to create."
)
@click.option(
    "--tls-key-file",
    type=str,
    default=DEFAULT_KEY_FILE,
    required=True,
    help="The TLS key file to create."
)
@click.option(
    "--user-list-filename",
    type=str,
    default=USER_LIST_FILENAME,
    show_default=True,
    required=True,
    help="The user dictionary file (in JSON) to store the created user data in.  This file should be used to start Sidewinder server afterward."
)
@click.option(
    "--secret-key",
    type=str,
    default=SECRET_KEY,
    show_default=False,
    required=True,
    help="The secret key used to salt the user password hashes.  The same key value MUST have been used when creating the user-list-file!"
)
@click.option(
    "--client-username",
    type=str,
    required=True,
    help="The client username to create for security purposes."
)
@click.option(
    "--client-password",
    type=str,
    required=True,
    help="The client password associated with the client username above"
)
@click.option(
    "--worker-password",
    type=str,
    required=True,
    help="The password which workers must use to connect to the server."
)
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
    "--shard-manifest-file",
    type=str,
    default=(DATA_DIR / "shards" / "manifests" / "local_tpch_sf1_shard_manifest.yaml").as_posix(),
    required=True,
    show_default=True,
    help="The output file path will have details about the shards created by this process."
)
@click.option(
    "--overwrite/--no-overwrite",
    type=bool,
    default=False,
    show_default=True,
    required=True,
    help="Can we overwrite the target database file if it already exists..."
)
def sidewinder_bootstrap(tls_cert_file: str,
                         tls_key_file: str,
                         user_list_filename: str,
                         secret_key: str,
                         client_username: str,
                         client_password: str,
                         worker_password: str,
                         tpch_scale_factor: float,
                         data_directory: str,
                         shard_count: int,
                         shard_manifest_file: str,
                         overwrite: bool
                         ):
    logger.info(msg=("Running sidewinder_bootstrap - with parameters: "
                     f"--tls-cert-file={tls_cert_file} "
                     f"--tls-key-file={tls_key_file} "
                     f"--user-list-filename={user_list_filename} "
                     f"--secret_key=(redacted) "
                     f"--client-username={tpch_scale_factor} "
                     f"--client-password=(redacted) "
                     f"--worker-password=(redacted) "
                     f"--tpch-scale-factor={tpch_scale_factor} "
                     f"--data-directory='{data_directory}' "
                     f"--shard_count={shard_count} "
                     f"--shard-manifest-file={shard_manifest_file} "
                     f"--overwrite={overwrite}"
                     )
                )
    # Create a server certificate key pair
    create_tls_keypair(cert_file=tls_cert_file,
                       key_file=tls_key_file,
                       overwrite=overwrite
                       )

    # Create a client user
    create_user(username=client_username,
                password=client_password,
                user_list_filename=user_list_filename,
                secret_key=secret_key
                )

    # Create a worker user
    create_user(username="worker",
                password=worker_password,
                user_list_filename=user_list_filename,
                secret_key=secret_key
                )

    parquet_path = generate_tpch_parquet_data(tpch_scale_factor=tpch_scale_factor,
                                              data_directory=data_directory,
                                              overwrite=overwrite
                                              )

    logger.info(msg=SEPARATOR)
    create_duckdb_database_from_parquet(benchmark_name="tpch",
                                        scale_factor=tpch_scale_factor,
                                        data_directory=data_directory,
                                        overwrite=overwrite)

    logger.info(msg=SEPARATOR)
    printable_tpch_scale_factor = get_printable_number(tpch_scale_factor)
    build_shards(shard_definition_file=(SCRIPT_DIR / "config" / "tpch_shard_generation_queries.yaml").as_posix(),
                 shard_count=shard_count,
                 shard_manifest_file=shard_manifest_file,
                 source_data_path=parquet_path.as_posix(),
                 output_data_path=(Path(data_directory) / "shards" / "tpch" / f"sf={printable_tpch_scale_factor}").as_posix(),
                 overwrite=overwrite
                 )

    logger.info(msg=SEPARATOR)
    logger.info(msg="All bootstrap steps completed")


if __name__ == "__main__":
    sidewinder_bootstrap()
