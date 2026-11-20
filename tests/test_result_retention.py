"""Result retention: disk-backed storage, bounded memory, eviction safety."""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.registry as registry_module
from app.main import app, registry, store
from app.registry import RoundNotFound, RoundRegistry, SUCCEEDED
from app.results import ResultStore, ResultStorageError
from app.worker import MSG_DONE, QueryWorker

SAMPLE = Path(__file__).resolve().parent.parent / "sample_data" / "observations.csv"


@pytest.fixture()
def client():
    registry._rounds.clear()
    store._dataset = None
    with TestClient(app) as c:
        yield c
    registry.shutdown()
    registry._rounds.clear()
    store._dataset = None


def _upload(client):
    with open(SAMPLE, "rb") as fh:
        resp = client.post(
            "/api/upload",
            files={"file": ("observations.csv", fh, "text/csv")},
        )
    assert resp.status_code == 200, resp.text


def _wait_terminal(client, round_id, timeout=20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = client.get(f"/api/rounds/{round_id}").json()
        if snap["state"] in ("succeeded", "failed", "cancelled"):
            return snap
        time.sleep(0.02)
    raise AssertionError("round did not finish")


def _run_round(client, sql, station="ST001"):
    resp = client.post("/api/rounds", json={"station_id": station, "sql": sql})
    assert resp.status_code == 201, resp.text
    round_id = resp.json()["round_id"]
    return round_id, _wait_terminal(client, round_id)


def _rows_columns(n_rows: int) -> dict[str, list]:
    return {
        "station_id": ["ST001"] * n_rows,
        "ts_utc": [f"2026-01-01T00:00:{i % 60:02d}Z" for i in range(n_rows)],
        "temperature_c": [float(i % 100) for i in range(n_rows)],
    }


def _registry(tmp_path) -> RoundRegistry:
    return RoundRegistry(results=ResultStore(str(tmp_path)))


# --------------------------------------------------------------------- basics
def test_result_is_on_disk_and_not_in_memory(client):
    _upload(client)
    round_id, final = _run_round(client, "SELECT * FROM observations")
    assert final["state"] == "succeeded"
    round_obj = registry.get(round_id)
    assert round_obj.result_path and os.path.isfile(round_obj.result_path)
    assert not hasattr(round_obj, "result_csv")
    # The published file name is derived from the round id and ends .csv.
    assert round_obj.result_path.endswith(f"{round_id}.csv")
    # No spool leftovers.
    names = os.listdir(registry._results.directory)
    assert not [n for n in names if n.endswith(".part")]


def test_download_streams_complete_file(client):
    _upload(client)
    round_id, final = _run_round(client, "SELECT * FROM observations")
    dl = client.get(f"/api/rounds/{round_id}/result")
    assert dl.status_code == 200
    assert int(dl.headers["content-length"]) == final["result_bytes"]
    assert len(dl.content) == final["result_bytes"]
    assert dl.content.startswith(b"station_id,ts_utc,temperature_c\n")


def test_failed_round_leaves_no_result_file(client):
    _upload(client)
    _registry_dir = registry._results.directory
    before = set(os.listdir(_registry_dir))
    round_id, final = _run_round(
        client,
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x < 200000) SELECT x, repeat('0123456789ABCDEF', 40) AS pad FROM seq",
    )
    assert final["state"] == "failed"
    assert "8 MiB" in final["error"]
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    # The oversized spool was removed rather than retained.
    after = set(os.listdir(_registry_dir))
    assert after == before


def test_worker_never_buffers_whole_csv(tmp_path):
    """A successful worker result carries only a path, not CSV bytes."""
    store_obj = ResultStore(str(tmp_path))
    worker = QueryWorker("a" * 32, "SELECT * FROM observations", _rows_columns(500), store_obj)
    worker.start()
    worker.join(timeout=10)
    assert worker.result is not None
    assert worker.result.kind == MSG_DONE
    assert worker.result.result_path and os.path.isfile(worker.result.result_path)
    assert not hasattr(worker.result, "csv_bytes")


# ---------------------------------------------------------------- publishing
def test_write_failure_marks_round_failed(client, monkeypatch):
    _upload(client)
    real = registry._results.new_spool

    def broken(round_id):
        raise ResultStorageError("disk full")

    monkeypatch.setattr(registry._results, "new_spool", broken)
    round_id, final = _run_round(client, "SELECT * FROM observations")
    assert final["state"] == "failed"
    assert "result storage failure" in final["error"]
    assert client.get(f"/api/rounds/{round_id}/result").status_code == 409
    monkeypatch.setattr(registry._results, "new_spool", real)


def test_publish_failure_marks_round_failed_and_discards_spool(client, monkeypatch, tmp_path):
    _upload(client)

    def broken(part_path, round_id):
        raise ResultStorageError("rename failed")

    monkeypatch.setattr(registry._results, "publish", broken)
    before = set(os.listdir(registry._results.directory))
    round_id, final = _run_round(client, "SELECT * FROM observations")
    assert final["state"] == "failed"
    assert "result storage failure" in final["error"]
    # No .part file was left behind by the failed publish.
    assert set(os.listdir(registry._results.directory)) == before


# ------------------------------------------------------------------ eviction
def test_eviction_keeps_last_100_and_removes_old_files(tmp_path, monkeypatch):
    monkeypatch.setattr(registry_module, "MAX_FINISHED_ROUNDS", 100)
    reg = _registry(tmp_path)
    reg._max_active = 2
    cols = _rows_columns(3)
    finished = []
    for _ in range(105):
        r = reg.submit("ST001", "SELECT 1 AS one", cols)
        r._worker.join(timeout=10)
        # Supervisor thread finalises right after the worker; wait for it.
        r._supervisor.join(timeout=10)
        assert r.state == SUCCEEDED
        finished.append(r.round_id)

    # 100 newest retained; the 5 oldest (and their files) are gone.
    assert len(reg._rounds) == 100
    old = finished[:5]
    new = finished[-100:]
    for rid in old:
        assert rid not in reg._rounds
        assert not os.path.exists(os.path.join(str(tmp_path), f"{rid}.csv"))
        with pytest.raises(RoundNotFound):
            reg.get(rid)
    for rid in new:
        assert os.path.exists(os.path.join(str(tmp_path), f"{rid}.csv"))
    assert len(os.listdir(tmp_path)) == 100


def test_evicted_round_is_404_but_open_download_survives(tmp_path):
    reg = _registry(tmp_path)
    cols = _rows_columns(1000)
    r = reg.submit("ST001", "SELECT * FROM observations", cols)
    r._worker.join(timeout=10)
    r._supervisor.join(timeout=10)
    assert r.state == SUCCEEDED

    # Open a descriptor as a download would, then evict the round.
    fd = reg.open_result(r.round_id)
    assert fd is not None
    reg._rounds.clear()
    for round_file in os.listdir(tmp_path):
        os.unlink(os.path.join(tmp_path, round_file))

    # The in-flight download can still read the whole file...
    chunks = list(reg.stream_result(fd))
    assert b"".join(chunks).startswith(b"station_id,ts_utc,temperature_c\n")
    # ...while new requests for the evicted round are rejected.
    with pytest.raises(RoundNotFound):
        reg.get(r.round_id)


def test_eviction_while_downloading_via_api(tmp_path, monkeypatch):
    """Evicting a round mid-download must not truncate the response."""
    store_obj = ResultStore(str(tmp_path))
    reg = RoundRegistry(results=store_obj)
    cols = _rows_columns(5000)
    r = reg.submit("ST001", "SELECT * FROM observations", cols)
    r._worker.join(timeout=15)
    r._supervisor.join(timeout=15)
    assert r.state == SUCCEEDED
    expected = open(r.result_path, "rb").read()

    fd = reg.open_result(r.round_id)
    first = os.read(fd, 1024)
    # Simulate eviction after the download has started.
    reg._rounds.clear()
    store_obj.remove(r.round_id)
    assert not os.path.exists(r.result_path)

    rest = b"".join(store_obj.stream_fd(fd))
    assert first + rest == expected


def test_active_rounds_are_never_evicted(tmp_path, monkeypatch):
    monkeypatch.setattr(registry_module, "MAX_FINISHED_ROUNDS", 1)
    reg = _registry(tmp_path)
    cols = _rows_columns(3)
    # Two long-running rounds keep both slots busy.
    slow = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq) "
        "SELECT count(*) FROM seq"
    )
    active = [reg.submit("ST001", slow, cols) for _ in range(2)]
    assert reg.active_count() == 2
    reg._evict_finished_locked()
    for r in active:
        assert r.round_id in reg._rounds
    for r in active:
        r._worker.interrupt("cleanup")
        r._worker.join(timeout=10)
        r._supervisor.join(timeout=10)


# --------------------------------------------------------------- directory IO
def test_result_store_rejects_unwritable_directory(tmp_path):
    target = tmp_path / "ro"
    target.mkdir()
    os.chmod(target, 0o500)
    try:
        with pytest.raises(ResultStorageError):
            store_obj = ResultStore(str(target / "sub"))
            store_obj.ensure_directory()
    finally:
        os.chmod(target, 0o700)


def test_client_disconnect_releases_descriptor(client):
    """ClientDisconnect must close the fd rather than leak it."""
    import errno

    _upload(client)
    round_id, final = _run_round(client, "SELECT * FROM observations")
    assert final["state"] == "succeeded"

    fd = registry.open_result(round_id)
    assert fd is not None
    # Closing through the generator's finally block frees the descriptor.
    gen = registry.stream_result(fd)
    next(gen)
    gen.close()
    try:
        os.fstat(fd)
    except OSError as exc:
        assert exc.errno == errno.EBADF
    else:
        raise AssertionError("descriptor was not released")


def test_stale_files_removed_at_startup(tmp_path):
    (tmp_path / "deadbeef.part").write_bytes(b"x")
    (tmp_path / "deadbeef.csv").write_bytes(b"x")
    (tmp_path / "keep.txt").write_text("keep")
    ResultStore(str(tmp_path))
    names = sorted(os.listdir(tmp_path))
    assert names == ["keep.txt"]
