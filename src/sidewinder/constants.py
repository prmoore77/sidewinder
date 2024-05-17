from pathlib import Path

# Constants
COMPLETED = "COMPLETED"
DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE = 1024 ** 3
DISTRIBUTED = "DISTRIBUTED"
ERROR = "Error"
FAILED = "FAILED"
INFO = "Info"
QUERY = "Query"
RESULT = "Result"
RUN_ON_SERVER = "RUN_ON_SERVER"
SHARD_REQUEST = "ShardRequest"
SHARD_CONFIRMATION = "ShardConfirmation"
SHARD_DATASET = "ShardDataset"
STARTED = "STARTED"
WORKER_FAILED = "FAILED"
WORKER_SUCCESS = "SUCCESS"
ARROW_RESULT_TYPE = "ARROW"
PARQUET_RESULT_TYPE = "PARQUET"
SHARD_URL_EXPIRATION_SECONDS = 5 * 60
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"

SERVER_PORT = 8765

# Directories
SECURITY_DIR = Path("security")

# File paths
USER_LIST_FILENAME = (SECURITY_DIR / "user_list.json").as_posix()