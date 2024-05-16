from __future__ import annotations

import asyncio
import os
import re
import signal
import ssl
import sys
import threading
from time import sleep
from typing import Any, Set

import click
import pandas as pd
from pglast import parser
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close
from websockets.legacy.client import connect

from . import __version__ as sidewinder_version
from .constants import SERVER_PORT
from .utils import get_dataframe_from_ipc_bytes

# Misc. Constants
SIDEWINDER_CLIENT_VERSION = sidewinder_version

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 0)
pd.set_option('display.max_colwidth', None)
# pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.precision', 99)


if sys.platform == "win32":

    def win_enable_vt100() -> None:
        """
        Enable VT-100 for console output on Windows.

        See also https://bugs.python.org/issue29059.

        """
        import ctypes

        STD_OUTPUT_HANDLE = ctypes.c_uint(-11)
        INVALID_HANDLE_VALUE = ctypes.c_uint(-1)
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x004

        handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        if handle == INVALID_HANDLE_VALUE:
            raise RuntimeError("unable to obtain stdout handle")

        cur_mode = ctypes.c_uint()
        if ctypes.windll.kernel32.GetConsoleMode(handle, ctypes.byref(cur_mode)) == 0:
            raise RuntimeError("unable to query current console mode")

        # ctypes ints lack support for the required bit-OR operation.
        # Temporarily convert to Py int, do the OR and convert back.
        py_int_mode = int.from_bytes(cur_mode, sys.byteorder)
        new_mode = ctypes.c_uint(py_int_mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING)

        if ctypes.windll.kernel32.SetConsoleMode(handle, new_mode) == 0:
            raise RuntimeError("unable to set console mode")


def exit_from_event_loop_thread(
        loop: asyncio.AbstractEventLoop,
        stop: asyncio.Future[None],
) -> None:
    loop.stop()
    if not stop.done():
        # When exiting the thread that runs the event loop, raise
        # KeyboardInterrupt in the main thread to exit the program.
        if sys.platform == "win32":
            ctrl_c = signal.CTRL_C_EVENT
        else:
            ctrl_c = signal.SIGINT
        os.kill(os.getpid(), ctrl_c)


def print_during_input(string: str) -> None:
    sys.stdout.write(
        # Save cursor position
        "\N{ESC}7"
        # Add a new line
        "\N{LINE FEED}"
        # Move cursor up
        "\N{ESC}[A"
        # Insert blank line, scroll last line down
        "\N{ESC}[L"
        # Print string in the inserted blank line
        f"{string}\N{LINE FEED}"
        # Restore cursor position
        "\N{ESC}8"
        # Move cursor down
        "\N{ESC}[B"
    )
    sys.stdout.flush()


def print_over_input(string: str) -> None:
    sys.stdout.write(
        # Move cursor to beginning of line
        "\N{CARRIAGE RETURN}"
        # Delete current line
        "\N{ESC}[K"
        # Print string
        f"{string}\N{LINE FEED}"
    )
    sys.stdout.flush()


async def is_sql_command(message: str) -> bool:
    try:
        tree = parser.parse_sql_json(message)
    except Exception as exc:
        return False
    else:
        return True


async def run_client(
        server_hostname: str,
        server_port: int,
        tls_verify: bool,
        tls_roots: str,
        mtls: list,
        mtls_password: str,
        username: str,
        password: str,
        max_result_set_rows: int,
        loop: asyncio.AbstractEventLoop,
        inputs: asyncio.Queue[str],
        stop: asyncio.Future[None],
) -> None:
    print(f"Starting Sidewinder Client - version: {SIDEWINDER_CLIENT_VERSION}")

    scheme = "wss"

    if mtls:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        mtls_cert_chain = mtls[0]
        mtls_private_key = mtls[1]
        ssl_context.load_cert_chain(certfile=mtls_cert_chain, keyfile=mtls_private_key, password=mtls_password)
    else:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    ssl_context.load_default_certs()

    if tls_verify:
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        if tls_roots:
            ssl_context.load_verify_locations(cafile=tls_roots)
    else:
        print("WARNING: TLS Verification is disabled.")
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    server_uri = f"{scheme}://{server_hostname}:{server_port}/client"
    print(f"Connecting to Server URI: {server_uri}")

    try:
        websocket = await connect(uri=server_uri,
                                  extra_headers=dict(),
                                  max_size=1024 ** 3,
                                  ssl=ssl_context
                                  )
    except Exception as exc:
        print_over_input(f"Failed to connect to {server_uri}: {exc}.")
        exit_from_event_loop_thread(loop, stop)
        return
    else:
        # Authenticate
        token = f"{username}:{password}"
        await websocket.send(message=token)

        print_during_input(f"Successfully connected to {server_uri} - as user: '{username}'.")

    try:
        while True:
            incoming: asyncio.Future[Any] = asyncio.create_task(websocket.recv())
            outgoing: asyncio.Future[Any] = asyncio.create_task(inputs.get())
            done: Set[asyncio.Future[Any]]
            pending: Set[asyncio.Future[Any]]
            done, pending = await asyncio.wait(
                [incoming, outgoing, stop], return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel pending tasks to avoid leaking them.
            if incoming in pending:
                incoming.cancel()
            if outgoing in pending:
                outgoing.cancel()

            if incoming in done:
                try:
                    message = incoming.result()
                except ConnectionClosed:
                    break
                else:
                    if isinstance(message, str):
                        print_during_input("< " + message)
                    else:
                        df = get_dataframe_from_ipc_bytes(bytes_value=message)
                        if (max_result_set_rows > 0) and (df.num_rows > max_result_set_rows):
                            print_during_input(f"Results (only displaying {max_result_set_rows:,} row(s)):\n{df.to_pandas().head(n=max_result_set_rows)}")
                        else:
                            print_during_input(f"Results:\n{df.to_pandas()}")

                        print_during_input(f"\n-----------\nResult set size: {df.num_rows:,} row(s) / {df.nbytes:,} bytes")

            if outgoing in done:
                message = outgoing.result()
                await websocket.send(message)

            if stop in done:
                break

    finally:
        await websocket.close()
        assert websocket.close_code is not None and websocket.close_reason is not None
        close_status = Close(websocket.close_code, websocket.close_reason)

        print_over_input(f"Connection closed: {close_status}.")

        exit_from_event_loop_thread(loop, stop)


@click.command()
@click.option(
    "--version/--no-version",
    type=bool,
    default=False,
    show_default=False,
    required=True,
    help="Prints the Sidewinder Client version and exits."
)
@click.option(
    "--server-hostname",
    type=str,
    default=os.getenv("SERVER_HOSTNAME", "localhost"),
    show_default=True,
    required=True,
    help="The hostname of the Sidewinder server."
)
@click.option(
    "--server-port",
    type=int,
    default=os.getenv("SERVER_PORT", SERVER_PORT),
    show_default=True,
    required=True,
    help="The port of the Sidewinder server."
)
@click.option(
    "--tls-verify/--no-tls-verify",
    type=bool,
    default=(os.getenv("TLS_VERIFY", "TRUE").upper() == "TRUE"),
    show_default=True,
    help="Verify the server's TLS certificate hostname and signature.  Using --no-tls-verify is insecure, only use for development purposes!"
)
@click.option(
    "--tls-roots",
    type=str,
    default=os.getenv("TLS_ROOTS"),
    show_default=True,
    help="'Path to trusted TLS certificate(s)"
)
@click.option(
    "--mtls",
    nargs=2,
    default=None,
    metavar=('CERTFILE', 'KEYFILE'),
    help="Enable transport-level security"
)
@click.option(
    "--mtls-password",
    type=str,
    required=False,
    help="The password for an encrypted client certificate private key (if needed)"
)
@click.option(
    "--username",
    type=str,
    default=os.getenv("CLIENT_USERNAME"),
    show_default=False,
    required=True,
    help="The client username to authenticate with."
)
@click.option(
    "--password",
    type=str,
    default=os.getenv("CLIENT_PASSWORD"),
    show_default=False,
    required=True,
    help="The client password associated with the username above"
)
@click.option(
    "--max-result-set-rows",
    type=int,
    default=100,
    show_default=True,
    required=True,
    help="The maximum number of rows to show in result sets.  A value of 0 means no limit."
)
def main(version: bool,
         server_hostname: str,
         server_port: int,
         tls_verify: bool,
         tls_roots: str,
         mtls: list,
         mtls_password: str,
         username: str,
         password: str,
         max_result_set_rows: int
         ) -> None:
    if version:
        print(f"Sidewinder Client - version: {sidewinder_version}")
        return

    # If we're on Windows, enable VT100 terminal support.
    if sys.platform == "win32":
        try:
            win_enable_vt100()
        except RuntimeError as exc:
            sys.stderr.write(
                f"Unable to set terminal to VT100 mode. This is only "
                f"supported since Win10 anniversary update. Expect "
                f"weird symbols on the terminal.\nError: {exc}\n"
            )
            sys.stderr.flush()

    try:
        import readline  # noqa
    except ImportError:  # Windows has no `readline` normally
        pass

    # Create an event loop that will run in a background thread.
    loop = asyncio.new_event_loop()

    # Due to zealous removal of the loop parameter in the Queue constructor,
    # we need a factory coroutine to run in the freshly created event loop.
    async def queue_factory() -> asyncio.Queue[str]:
        return asyncio.Queue()

    # Create a queue of user inputs. There's no need to limit its size.
    inputs: asyncio.Queue[str] = loop.run_until_complete(queue_factory())

    # Create a stop condition when receiving SIGINT or SIGTERM.
    stop: asyncio.Future[None] = loop.create_future()

    # Schedule the task that will manage the connection.
    loop.create_task(run_client(server_hostname=server_hostname,
                                server_port=server_port,
                                tls_verify=tls_verify,
                                tls_roots=tls_roots,
                                mtls=mtls,
                                mtls_password=mtls_password,
                                username=username,
                                password=password,
                                max_result_set_rows=max_result_set_rows,
                                loop=loop,
                                inputs=inputs,
                                stop=stop,
                                )
                     )

    # Start the event loop in a background thread.
    thread = threading.Thread(target=loop.run_forever)
    thread.start()

    # Read from stdin in the main thread in order to receive signals.
    try:
        while True:
            lines = []
            prompt_str = "> "
            while True:
                line = input(prompt_str)
                if line:
                    prompt_str = ". "
                    lines.append(line)

                # Search for a SQL terminator
                if re.search(r"(;|^/)\s*$", line):
                    break
            message = '\n'.join(lines)
            loop.call_soon_threadsafe(inputs.put_nowait, message)
    except (KeyboardInterrupt, EOFError):  # ^C, ^D
        # Sleep for a second in case the EOF was called in a bash script
        sleep(1)

        loop.call_soon_threadsafe(stop.set_result, None)

    # Wait for the event loop to terminate.
    thread.join()

    # For reasons unclear, even though the loop is closed in the thread,
    # it still thinks it's running here.
    loop.close()


if __name__ == "__main__":
    main()
