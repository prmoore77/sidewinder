from pathlib import Path
import duckdb
from ..config import logger
import contextlib
import os


# Constants
SCRIPT_DIR = Path(__file__).parent.resolve()


def execute_query(conn: duckdb.DuckDBPyConnection,
                  query: str
                  ):
    logger.info(msg=f"Executing SQL: '{query}'")
    conn.execute(query=query)


def get_printable_number(num: float):
    return '{:.9g}'.format(num)


@contextlib.contextmanager
def pushd(new_dir):
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(previous_dir)
