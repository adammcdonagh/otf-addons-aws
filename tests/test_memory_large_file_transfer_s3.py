# pylint: skip-file
# ruff: noqa
"""Memory utilization test for OTF batch S3 proxy transfers.

Creates a batch of 10 sequential S3 proxy transfers, each moving a unique 1 MB file
from BUCKET_SRC (src/) to BUCKET_DST (dest/) via a proxy transfer where the file
is pulled to the local worker and then uploaded — NOT an S3-to-S3 server-side copy.

Memory growth is driven by Python handler object accumulation (botocore clients,
loggers, etc.), not by file size, so small files are sufficient to catch regressions
while keeping the test fast enough for regular CI runs.

RSS memory is sampled every MEMORY_SAMPLE_INTERVAL seconds in a background thread
and written to:

  * the console (printed via pytest -s)
  * /tmp/otf_s3_mem_<batch_id>.log  (CSV: timestamp, elapsed_s, rss_mb)

Source objects are created from sparse local files (negligible local disk usage).
Both source and destination S3 objects are deleted in fixture teardown so the
floci state is left clean.

NOTE: This test requires the floci docker service (see tests/docker-compose.yml).

Run in isolation with visible output:
    pytest tests/test_memory_large_file_transfer_s3.py -v -s
"""

import ctypes
import datetime
import gc
import json
import logging
import os
import re
import subprocess
import threading
import time
import uuid

import boto3
import objgraph
import opentaskpy.otflogging
import psutil
import pytest
from opentaskpy.config.loader import ConfigLoader
from opentaskpy.taskhandlers import batch

from tests.fixtures.localstack import *  # noqa: F403

os.environ["OTF_LOG_LEVEL"] = "INFO"
os.environ["OTF_NO_LOG"] = "1"  # do not write log files to the local overlay filesystem
os.environ["OTF_BATCH_POLL_INTERVAL"] = (
    "0.1"  # don't wait 5s between batch status checks
)
os.environ["OTF_NO_THREAD_SLEEP"] = "1"  # don't wait 1s between task thread creation

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_TASKS = 10
FILE_SIZE_BYTES = (
    1 * 1024 * 1024
)  # 1 MB — small enough for fast CI runs; memory growth is handler-driven, not file-size-driven
MEMORY_SAMPLE_INTERVAL = 2  # seconds between RSS samples
TRANSFER_TIMEOUT = 120  # seconds per task

# Memory regression threshold.  Current baseline is ~5 MB/task after fixes;
# 15 MB/task gives generous headroom while catching any return of the original
# ~24 MB/task leak caused by un-nulled task_handler references in batch.py.
MAX_UNRECOVERABLE_GROWTH_PER_TASK_MB = 15

BUCKET_SRC = "otf-mem-test-src"
BUCKET_DST = "otf-mem-test-dst"
S3_SRC_DIR = "src"
S3_DST_DIR = "dest"
S3_PROTOCOL = "opentaskpy.addons.aws.remotehandlers.s3.S3Transfer"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_sparse_local_file(path: str, size: int) -> None:
    """Create a sparse file at *path* of *size* bytes.

    On Linux the OS allocates no real disk blocks for the gap, so local disk
    usage is negligible while the file reads back as exactly *size* zeroes.
    The S3 put_object call streams those zeroes to floci, simulating a
    realistic large-file upload without exhausting local disk space.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.seek(size - 1)
        fh.write(b"\x00")


class MemoryMonitor:
    """Samples RSS memory of the current process in a background thread.

    Usage::

        monitor = MemoryMonitor("/tmp/mem.log")
        initial = monitor.current_rss_mb()
        monitor.start()
        # ... run workload ...
        monitor.stop()
        summary = monitor.summary()
    """

    def __init__(self, log_file: str, sample_interval: float = 2.0) -> None:
        self._log_file = log_file
        self._sample_interval = sample_interval
        self._stop_event = threading.Event()
        self._samples: list[tuple[float, float]] = []  # (elapsed_s, rss_mb)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._process = psutil.Process(os.getpid())
        self._start_time: float = 0.0

    def start(self) -> None:
        self._start_time = time.monotonic()
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join()

    def current_rss_mb(self) -> float:
        return self._process.memory_info().rss / (1024 * 1024)

    def _run(self) -> None:
        with open(self._log_file, "w", encoding="utf-8") as fh:
            fh.write("timestamp,elapsed_s,rss_mb\n")
            while not self._stop_event.is_set():
                elapsed = time.monotonic() - self._start_time
                rss_mb = self.current_rss_mb()
                ts = datetime.datetime.now().isoformat(timespec="seconds")
                line = f"{ts},{elapsed:.1f},{rss_mb:.1f}\n"
                fh.write(line)
                fh.flush()
                self._samples.append((elapsed, rss_mb))
                self._stop_event.wait(self._sample_interval)

    def summary(self) -> dict:
        if not self._samples:
            return {}
        rss_values = [s[1] for s in self._samples]
        return {
            "min_mb": min(rss_values),
            "max_mb": max(rss_values),
            "final_mb": rss_values[-1],
            "samples": len(rss_values),
        }


# ---------------------------------------------------------------------------
# Task / batch config builders
# ---------------------------------------------------------------------------


def _transfer_task_definition(file_name: str) -> dict:
    """Return a proxy S3-to-S3 transfer task definition for *file_name*.

    ``transferType: proxy`` forces OTF to pull the object to the local worker
    and re-upload it, rather than using the S3 server-side copy API.
    """
    return {
        "type": "transfer",
        "source": {
            "bucket": BUCKET_SRC,
            "directory": S3_SRC_DIR,
            "fileRegex": re.escape(file_name),
            "protocol": {
                "name": S3_PROTOCOL,
            },
        },
        "destination": [
            {
                "bucket": BUCKET_DST,
                "directory": S3_DST_DIR,
                "transferType": "proxy",
                "protocol": {
                    "name": S3_PROTOCOL,
                },
            }
        ],
    }


def _batch_definition(task_ids: list) -> dict:
    """Return a sequential batch where each task depends on the previous one."""
    tasks = []
    for i, task_id in enumerate(task_ids, start=1):
        entry: dict = {
            "order_id": i,
            "task_id": task_id,
            "timeout": TRANSFER_TIMEOUT,
        }
        if i > 1:
            entry["dependencies"] = [i - 1]
        tasks.append(entry)
    return {"type": "batch", "tasks": tasks}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def memory_test_s3_config(tmp_path):
    """Write unique task + batch JSON config files to a temporary directory.

    Returns a 5-tuple:
        (config_dir: str, batch_id: str, task_ids: list[str],
         file_names: list[str], object_keys: list[str])

    Each object key is the S3 key used under S3_SRC_DIR (e.g. ``src/mem_test_abc_1.dat``).
    """
    run_id = uuid.uuid4().hex[:8]
    task_ids = [f"mem-s3-{run_id}-{i}" for i in range(1, NUM_TASKS + 1)]
    file_names = [f"mem_test_{run_id}_{i}.dat" for i in range(1, NUM_TASKS + 1)]
    object_keys = [f"{S3_SRC_DIR}/{fn}" for fn in file_names]
    batch_id = f"mem-s3-batch-{run_id}"

    # ConfigLoader requires a variables.json to be present
    (tmp_path / "variables.json").write_text("{}", encoding="utf-8")

    # Write one transfer JSON file per task
    for task_id, file_name in zip(task_ids, file_names):
        task_def = _transfer_task_definition(file_name)
        (tmp_path / f"{task_id}.json").write_text(
            json.dumps(task_def, indent=2), encoding="utf-8"
        )

    # Write the batch JSON file
    batch_def = _batch_definition(task_ids)
    (tmp_path / f"{batch_id}.json").write_text(
        json.dumps(batch_def, indent=2), encoding="utf-8"
    )

    return str(tmp_path), batch_id, task_ids, file_names, object_keys


@pytest.fixture(scope="function")
def setup_mem_buckets(credentials, s3_client):
    """Create the source and destination buckets, clean up after test."""
    for bucket in [BUCKET_SRC, BUCKET_DST]:
        subprocess.run(
            ["awslocal", "s3", "rb", f"s3://{bucket}", "--force"], check=False
        )
        subprocess.run(["awslocal", "s3", "mb", f"s3://{bucket}"], check=False)

    yield

    for bucket in [BUCKET_SRC, BUCKET_DST]:
        subprocess.run(
            ["awslocal", "s3", "rb", f"s3://{bucket}", "--force"], check=False
        )


@pytest.fixture(scope="function")
def large_source_objects(tmp_path, s3_client, memory_test_s3_config, setup_mem_buckets):
    """Upload 2 GB sparse files as S3 objects to BUCKET_SRC, clean up in teardown.

    Uses a temporary local sparse file per object (minimal local disk usage).
    Destination objects in BUCKET_DST are also deleted in teardown.
    """
    _, _, _, file_names, object_keys = memory_test_s3_config

    uploaded_keys = []
    for file_name, object_key in zip(file_names, object_keys):
        local_path = str(tmp_path / file_name)
        _create_sparse_local_file(local_path, FILE_SIZE_BYTES)

        print(
            f"\n  [setup] Uploading sparse object: s3://{BUCKET_SRC}/{object_key} "
            f"({FILE_SIZE_BYTES / (1024 ** 2):.0f} MB)"
        )
        # upload_file uses the boto3 S3 transfer manager which automatically
        # applies multipart upload for large files (default threshold 8 MB).
        # This is far more reliable than put_object for files > ~100 MB.
        s3_client.upload_file(local_path, BUCKET_SRC, object_key)

        # Remove local sparse file immediately after upload to free disk space
        os.remove(local_path)
        uploaded_keys.append(object_key)

    yield uploaded_keys

    # --- teardown: delete source objects ---
    for key in uploaded_keys:
        try:
            s3_client.delete_object(Bucket=BUCKET_SRC, Key=key)
            print(f"  [teardown] Deleted source     : s3://{BUCKET_SRC}/{key}")
        except Exception as exc:
            print(
                f"  [teardown] Could not delete source s3://{BUCKET_SRC}/{key}: {exc}"
            )

    # --- teardown: delete destination objects ---
    for file_name in file_names:
        dst_key = f"{S3_DST_DIR}/{file_name}"
        try:
            s3_client.delete_object(Bucket=BUCKET_DST, Key=dst_key)
            print(f"  [teardown] Deleted destination: s3://{BUCKET_DST}/{dst_key}")
        except Exception as exc:
            print(
                f"  [teardown] Could not delete destination s3://{BUCKET_DST}/{dst_key}: {exc}"
            )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_memory_usage_large_file_batch_s3_proxy_transfer(
    s3_client,
    memory_test_s3_config,
    large_source_objects,
):
    """Run 10 sequential 2 GB S3 proxy transfers as a batch and monitor RSS memory.

    Each task uses ``transferType: proxy`` so OTF downloads the object to the
    local worker first and re-uploads it, rather than issuing a server-side copy.
    This exercises the same code paths as non-S3 transfers for memory pressure.

    The test:
    1. Writes 10 unique task JSON configs + 1 batch JSON config to a tmp dir.
    2. Uploads a 2 GB sparse object to BUCKET_SRC for each task.
    3. Starts a background thread sampling RSS every MEMORY_SAMPLE_INTERVAL s.
    4. Runs the batch via ConfigLoader + batch.Batch.run().
    5. Prints a memory summary to the console and writes a CSV log to /tmp.
    6. Asserts all transfers succeeded and all destination objects exist.
    7. Deletes source and destination objects in fixture teardown.
    """
    config_dir, batch_id, task_ids, file_names, _ = memory_test_s3_config
    log_file = f"/tmp/otf_s3_mem_{batch_id}.log"

    print(f"\n[memory-test-s3] Memory log  : {log_file}")
    print(f"[memory-test-s3] Config dir  : {config_dir}")
    print(f"[memory-test-s3] Batch ID    : {batch_id}")
    print(
        f"[memory-test-s3] {NUM_TASKS} tasks × {FILE_SIZE_BYTES / (1024 ** 2):.0f} MB each "
        f"({FILE_SIZE_BYTES * NUM_TASKS / (1024 ** 2):.0f} MB total) — proxy transfer"
    )
    print(f"[memory-test-s3] Source      : s3://{BUCKET_SRC}/{S3_SRC_DIR}/")
    print(f"[memory-test-s3] Destination : s3://{BUCKET_DST}/{S3_DST_DIR}/")

    config_loader = ConfigLoader(config_dir)
    batch_definition = config_loader.load_task_definition(batch_id)

    # --- profiling: baseline before batch ---
    gc.collect()
    objgraph.show_growth(limit=0)  # establish baseline (discarded)
    loggers_before = sum(
        1
        for v in logging.Logger.manager.loggerDict.values()
        if isinstance(v, logging.Logger)
    )

    monitor = MemoryMonitor(log_file=log_file, sample_interval=MEMORY_SAMPLE_INTERVAL)
    initial_rss = monitor.current_rss_mb()
    print(f"[memory-test-s3] Initial RSS : {initial_rss:.1f} MB")

    monitor.start()
    try:
        batch_obj = batch.Batch(None, batch_id, batch_definition, config_loader)
        result = batch_obj.run()
    finally:
        monitor.stop()

    final_rss = monitor.current_rss_mb()
    summary = monitor.summary()
    growth = final_rss - initial_rss

    print()
    print("[memory-test-s3] ========== Memory Usage Summary ==========")
    print(f"[memory-test-s3] Initial RSS : {initial_rss:.1f} MB")
    print(f"[memory-test-s3] Final RSS   : {final_rss:.1f} MB")
    print(f"[memory-test-s3] Peak RSS    : {summary.get('max_mb', 0):.1f} MB")
    print(f"[memory-test-s3] Min RSS     : {summary.get('min_mb', 0):.1f} MB")
    print(f"[memory-test-s3] Growth      : {growth:+.1f} MB")
    print(f"[memory-test-s3] Samples     : {summary.get('samples', 0)}")
    print(f"[memory-test-s3] Log file    : {log_file}")
    print("[memory-test-s3] =============================================")

    # --- gc analysis ---
    rss_before_gc = monitor.current_rss_mb()
    collected = gc.collect()
    rss_after_gc = monitor.current_rss_mb()
    print()
    print(f"[gc] Objects collected by gc.collect() : {collected}")
    print(f"[gc] RSS before gc.collect()           : {rss_before_gc:.1f} MB")
    print(f"[gc] RSS after  gc.collect()           : {rss_after_gc:.1f} MB")
    print(
        f"[gc] RSS freed by gc                   : {rss_before_gc - rss_after_gc:+.1f} MB"
    )

    # --- malloc_trim: ask glibc to return freed arenas to the OS ---
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
        rss_after_trim = monitor.current_rss_mb()
        print(f"[gc] RSS after  malloc_trim(0)        : {rss_after_trim:.1f} MB")
        print(
            f"[gc] RSS freed by malloc_trim         : {rss_after_gc - rss_after_trim:+.1f} MB"
        )
        print(
            f"[gc] Unrecoverable RSS growth         : {rss_after_trim - initial_rss:+.1f} MB"
        )
    except Exception as e:
        print(f"[gc] malloc_trim not available: {e}")

    # --- objgraph: what object types grew? ---
    print()
    print("[objgraph] Object type growth during batch (top 20):")
    objgraph.show_growth(limit=20)

    # --- logger accumulation ---
    loggers_after = sum(
        1
        for v in logging.Logger.manager.loggerDict.values()
        if isinstance(v, logging.Logger)
    )
    print()
    print(f"[loggers] Logger count before batch : {loggers_before}")
    print(f"[loggers] Logger count after  batch : {loggers_after}")
    print(f"[loggers] New loggers created       : {loggers_after - loggers_before}")

    # --- assert all transfers completed successfully ---
    assert result, "Batch of large-file S3 proxy transfers reported failure"

    for file_name in file_names:
        dst_key = f"{S3_DST_DIR}/{file_name}"
        response = s3_client.head_object(Bucket=BUCKET_DST, Key=dst_key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, (
            f"Expected destination object not found after transfer: "
            f"s3://{BUCKET_DST}/{dst_key}"
        )

    # --- memory regression assertion ---
    # Use the post-malloc_trim RSS as the "true" retained memory; fall back to
    # post-gc RSS when malloc_trim is unavailable (non-Linux environments).
    try:
        unrecoverable_mb = rss_after_trim - initial_rss
    except NameError:
        unrecoverable_mb = rss_after_gc - initial_rss

    growth_per_task_mb = unrecoverable_mb / NUM_TASKS
    print()
    print(f"[assert] Unrecoverable growth per task : {growth_per_task_mb:.1f} MB")
    print(
        f"[assert] Threshold                     : {MAX_UNRECOVERABLE_GROWTH_PER_TASK_MB} MB/task"
    )
    assert growth_per_task_mb < MAX_UNRECOVERABLE_GROWTH_PER_TASK_MB, (
        f"Memory regression detected: {growth_per_task_mb:.1f} MB/task retained after "
        f"gc.collect() + malloc_trim (threshold: {MAX_UNRECOVERABLE_GROWTH_PER_TASK_MB} MB/task). "
        f"Total unrecoverable growth: {unrecoverable_mb:.1f} MB over {NUM_TASKS} tasks."
    )
