"""Global limits and constants for the SQL sandpit service."""
from __future__ import annotations

import os
import tempfile

# Upload limits
MAX_UPLOAD_BYTES = 1 * 1024 * 1024  # 1 MiB CSV
MAX_CSV_ROWS = 200_000

# Concurrency: at most two rounds executing at the same time.
MAX_CONCURRENT_ROUNDS = 2

# Execution limits.
QUERY_TIMEOUT_SECONDS = 5.0
# Internal hard guard slightly above the advertised 5 s deadline; the
# supervisor also terminates the process at the advertised deadline.
HARD_DEADLINE_SECONDS = 5.5
MAX_RESULT_BYTES = 8 * 1024 * 1024  # 8 MiB result CSV
MAX_SQL_LENGTH = 100_000

# The only table visible inside a round.
TABLE_NAME = "observations"
TABLE_COLUMNS = ("station_id", "ts_utc", "temperature_c")

# How many rows a worker pulls / serialises per batch.
BATCH_SIZE = 2048

# Keep finished round metadata even after their slots are released. Only the
# most recent MAX_FINISHED_ROUNDS finished rounds remain queryable; when a
# round is evicted its result file is deleted as well.
MAX_FINISHED_ROUNDS = 100

# Successful result CSVs are spooled/published on disk rather than kept in the
# Python heap, so retaining history does not grow process memory. The directory
# must be writable; docker-compose mounts a volume here because the container
# root filesystem is read-only.
RESULT_DIR = os.environ.get(
    "RESULT_DIR",
    os.path.join(tempfile.gettempdir(), "sql-sandbox-results"),
)
