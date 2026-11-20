"""On-disk retention of successful result CSVs.

Results are never kept as Python ``bytes`` once a round finishes. The worker
writes the CSV into a private spool file while the query runs; a successful
round then *publishes* that file under its round id. Downloads stream from the
published file in bounded chunks, and eviction unlinks it.

Process memory therefore stays bounded regardless of how much result history
exists: at most one batch-sized chunk per in-flight query / download lives in
the heap.

File lifecycle
--------------
* ``<round_id>.part`` -- being written by a running worker (private).
* ``<round_id>.csv``  -- published, complete, downloadable.
* unknown names      -- leftovers of a crashed process, removed at startup.

Eviction is safe with respect to in-flight downloads: on POSIX, unlinking a
file does not invalidate an already-open descriptor, so a streaming response
that opened the file before eviction can read it to the end. Requests that
arrive after eviction get 404 because the round registration is gone.
"""
from __future__ import annotations

import os
from typing import BinaryIO, Iterator, Optional

from . import config as config_module

# Size of each chunk read from disk and yielded to the HTTP response. Kept
# small so concurrent downloads do not pin megabytes in the process.
DOWNLOAD_CHUNK_BYTES = 256 * 1024
# Buffering of the worker's spool file.
SPOOL_BUFFER_BYTES = 64 * 1024


class ResultStorageError(RuntimeError):
    """Raised when a result file cannot be created, written or published."""


def _part_path(directory: str, round_id: str) -> str:
    return os.path.join(directory, f"{round_id}.part")


def _final_path(directory: str, round_id: str) -> str:
    return os.path.join(directory, f"{round_id}.csv")


class ResultStore:
    """Owns the result directory and all files inside it."""

    def __init__(self, directory: Optional[str] = None) -> None:
        # Resolve at construction time so tests / deployments can retarget
        # RESULT_DIR without reimporting the module.
        self.directory = directory or config_module.RESULT_DIR
        self.ensure_directory()
        # Only one process (uvicorn --workers 1) owns the directory.
        self._remove_stale_files()

    def ensure_directory(self) -> None:
        try:
            os.makedirs(self.directory, exist_ok=True)
        except OSError as exc:
            raise ResultStorageError(
                f"cannot create result directory {self.directory!r}: {exc}"
            ) from exc

    # ------------------------------------------------------------- lifecycle
    def _remove_stale_files(self) -> None:
        """Remove spool/published files left by a previous process.

        They are not reachable through the API after a restart (the round
        registry is in memory), so they would only waste space.
        """
        try:
            names = os.listdir(self.directory)
        except OSError:
            return
        for name in names:
            if name.endswith(".part") or name.endswith(".csv"):
                try:
                    os.unlink(os.path.join(self.directory, name))
                except OSError:
                    pass

    # --------------------------------------------------------------- spooling
    def new_spool(self, round_id: str) -> tuple[BinaryIO, str]:
        """Open a private spool file for the worker of ``round_id``."""
        part = _part_path(self.directory, round_id)
        try:
            fh = open(part, "wb", buffering=SPOOL_BUFFER_BYTES)
        except OSError as exc:
            raise ResultStorageError(f"cannot create result spool: {exc}") from exc
        return fh, part

    def discard(self, path: Optional[str]) -> None:
        """Delete a file, ignoring races with eviction or shutdown."""
        if not path:
            return
        try:
            os.unlink(path)
        except OSError:
            pass

    def discard_spool_of(self, round_id: str) -> None:
        """Delete any unpublished spool file for a completed round."""
        self.discard(_part_path(self.directory, round_id))

    def publish(self, part_path: str, round_id: str) -> str:
        """Atomically expose a completed spool file under the round id.

        Downloads resolve the final name only after this rename, so a partially
        written file can never be served.
        """
        final = _final_path(self.directory, round_id)
        try:
            os.replace(part_path, final)
        except OSError as exc:
            self.discard(part_path)
            raise ResultStorageError(f"cannot publish result: {exc}") from exc
        return final

    # ---------------------------------------------------------------- removal
    def remove(self, round_id: str) -> None:
        """Drop every file belonging to a round (called on eviction)."""
        for path in (
            _part_path(self.directory, round_id),
            _final_path(self.directory, round_id),
        ):
            try:
                os.unlink(path)
            except OSError:
                pass

    # --------------------------------------------------------------- download
    def open_download(self, round_id: str) -> Optional[int]:
        """Open the published result for reading.

        Returns a descriptor that stays valid even if the file is unlinked by a
        concurrent eviction; the caller owns and must close it.
        """
        try:
            return os.open(_final_path(self.directory, round_id), os.O_RDONLY)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ResultStorageError(f"cannot open result: {exc}") from exc

    def stream_fd(self, fd: int) -> Iterator[bytes]:
        """Yield the file contents in bounded chunks; closes the descriptor."""
        try:
            while True:
                chunk = os.read(fd, DOWNLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
