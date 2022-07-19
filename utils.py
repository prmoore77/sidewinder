import asyncio
import base64
from functools import wraps

import pyarrow
import duckdb
from config import logger


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def get_dataframe_from_bytes(bytes_value: bytes) -> pyarrow.Table:
    return pyarrow.ipc.open_stream(bytes_value).read_all()


def get_dataframe_bytes(df: pyarrow.Table) -> bytes:
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, df.schema) as writer:
        writer.write(df)
    buf = sink.getvalue()
    return buf.to_pybytes()


def get_dataframe_results_as_base64_str(df: pyarrow.Table) -> str:
    return base64.b64encode(get_dataframe_bytes(df)).decode()


def combine_bytes_results(result_bytes_list, summary_query) -> bytes:
    table_list = []
    for result_bytes in result_bytes_list:
        table_list.append(get_dataframe_from_bytes(bytes_value=result_bytes))

    combined_result = pyarrow.concat_tables(tables=table_list)

    con = duckdb.connect(database=':memory:')

    logger.info(msg=f"Running summarization query: '{summary_query}'")
    summarized_result = con.execute(summary_query).fetch_arrow_table()

    return get_dataframe_bytes(df=summarized_result)
