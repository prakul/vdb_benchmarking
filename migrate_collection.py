#!/usr/bin/env python3
"""
MongoDB Atlas Collection Migration Script

Migrates a collection from one Atlas cluster to another with:
- Parallel worker threads for throughput
- Batched bulk inserts
- Index replication
- Resumable checkpointing
- Progress reporting
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, OperationFailure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class MigrationConfig:
    src_uri: str
    dst_uri: str
    src_db: str
    dst_db: str
    collection: str
    dst_collection: Optional[str] = None   # if set, write to this name instead of collection
    batch_size: int = 1000          # documents per bulk insert
    read_batch_size: int = 5000     # cursor batch size from source
    num_workers: int = 8            # parallel insert threads
    queue_depth: int = 50           # max batches queued between reader and writers
    checkpoint_file: Optional[str] = None   # path to resume file (optional)
    upsert: bool = False            # upsert instead of insert (idempotent re-runs)
    copy_indexes: bool = True
    skip_id_index: bool = True      # _id index is auto-created, skip to avoid errors
    src_query: dict = field(default_factory=dict)    # optional filter on source
    src_projection: Optional[dict] = None            # optional projection
    src_sort: Optional[list] = None                  # e.g. [("_id", 1)]
    src_read_preference: str = "secondaryPreferred"  # reduce load on primary


# ---------------------------------------------------------------------------
# Checkpoint (resume support)
# ---------------------------------------------------------------------------

class Checkpoint:
    """Persists the last successfully inserted _id so migration can resume."""

    def __init__(self, path: Optional[str]):
        self.path = Path(path) if path else None
        self.last_id = None
        self._lock = threading.Lock()
        if self.path and self.path.exists():
            self._load()

    def _load(self):
        try:
            data = json.loads(self.path.read_text())
            self.last_id = data.get("last_id")
            if self.last_id:
                log.info("Resuming from checkpoint: last_id=%s", self.last_id)
        except Exception as exc:
            log.warning("Could not read checkpoint file: %s", exc)

    def save(self, last_id):
        if not self.path:
            return
        with self._lock:
            self.last_id = last_id
            try:
                self.path.write_text(json.dumps({"last_id": str(last_id), "ts": datetime.utcnow().isoformat()}))
            except Exception as exc:
                log.warning("Checkpoint write failed: %s", exc)

    def clear(self):
        if self.path and self.path.exists():
            self.path.unlink()


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    inserted: int = 0
    skipped: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, inserted=0, skipped=0, errors=0):
        with self._lock:
            self.inserted += inserted
            self.skipped += skipped
            self.errors += errors


# ---------------------------------------------------------------------------
# Index helpers
# ---------------------------------------------------------------------------

def replicate_indexes(src_col, dst_col, skip_id: bool):
    """Copy all indexes from source collection to destination."""
    indexes = list(src_col.list_indexes())
    for idx in indexes:
        name = idx.get("name", "")
        if skip_id and name == "_id_":
            continue
        keys = list(idx["key"].items())
        options = {
            k: v for k, v in idx.items()
            if k not in ("key", "ns", "v")
        }
        options.pop("name", None)   # let pymongo derive or keep explicit
        try:
            dst_col.create_index(keys, name=name, **options)
            log.info("  Created index: %s", name)
        except OperationFailure as exc:
            log.warning("  Could not create index '%s': %s", name, exc)


# ---------------------------------------------------------------------------
# Reader (single thread, feeds the queue)
# ---------------------------------------------------------------------------

def reader_thread(src_col, cfg: MigrationConfig, queue: Queue, checkpoint: Checkpoint, stats: Stats, stop_event: threading.Event):
    query = dict(cfg.src_query)

    # Resume: skip documents already migrated
    if checkpoint.last_id is not None:
        query["_id"] = {"$gt": checkpoint.last_id}
        log.info("Resuming: filtering _id > %s", checkpoint.last_id)

    sort = cfg.src_sort or [("_id", 1)]
    cursor = (
        src_col
        .find(query, cfg.src_projection, sort=sort)
        .batch_size(cfg.read_batch_size)
        .allow_disk_use(True)
    )

    batch = []
    for doc in cursor:
        if stop_event.is_set():
            break
        batch.append(doc)
        if len(batch) >= cfg.batch_size:
            queue.put(batch)
            batch = []

    if batch:
        queue.put(batch)

    queue.put(None)   # sentinel: no more data


# ---------------------------------------------------------------------------
# Writer workers
# ---------------------------------------------------------------------------

def writer_thread(dst_col, queue: Queue, checkpoint: Checkpoint, stats: Stats, stop_event: threading.Event, upsert: bool):
    while not stop_event.is_set():
        try:
            batch = queue.get(timeout=2)
        except Empty:
            continue

        if batch is None:
            # Propagate sentinel so other workers also stop
            queue.put(None)
            break

        try:
            _write_batch(dst_col, batch, checkpoint, stats, upsert)
        except Exception as exc:
            log.error("Unhandled error in writer: %s", exc)
            stats.add(errors=len(batch))
        finally:
            queue.task_done()


def _write_batch(dst_col, batch: list, checkpoint: Checkpoint, stats: Stats, upsert: bool):
    if upsert:
        ops = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            for doc in batch
        ]
        try:
            result = dst_col.bulk_write(ops, ordered=False)
            inserted = result.upserted_count + result.modified_count + result.matched_count
            stats.add(inserted=inserted)
        except BulkWriteError as exc:
            n_err = len(exc.details.get("writeErrors", []))
            stats.add(inserted=len(batch) - n_err, errors=n_err)
            log.warning("Bulk upsert partial error (%d docs): %s", n_err, exc.details.get("writeErrors", [])[:3])
    else:
        try:
            result = dst_col.insert_many(batch, ordered=False)
            stats.add(inserted=len(result.inserted_ids))
        except BulkWriteError as exc:
            n_ok = exc.details.get("nInserted", 0)
            n_err = len(exc.details.get("writeErrors", []))
            # Duplicate key errors (11000) during resume are expected — count as skipped
            dup_errors = [e for e in exc.details.get("writeErrors", []) if e.get("code") == 11000]
            real_errors = n_err - len(dup_errors)
            stats.add(inserted=n_ok, skipped=len(dup_errors), errors=real_errors)
            if real_errors:
                log.warning("Bulk insert errors (%d): %s", real_errors, exc.details.get("writeErrors", [])[:3])

    # Checkpoint on the last _id of this batch (best-effort ordering)
    try:
        checkpoint.save(batch[-1]["_id"])
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Progress reporter
# ---------------------------------------------------------------------------

def progress_reporter(stats: Stats, stop_event: threading.Event, total: Optional[int], interval: float = 5.0):
    start = time.monotonic()
    while not stop_event.is_set():
        time.sleep(interval)
        elapsed = time.monotonic() - start
        rate = stats.inserted / elapsed if elapsed > 0 else 0
        pct = f"{stats.inserted / total * 100:.1f}%" if total else "?"
        log.info(
            "Progress: %d inserted | %d skipped | %d errors | %.0f docs/s | %s complete",
            stats.inserted, stats.skipped, stats.errors, rate, pct,
        )


# ---------------------------------------------------------------------------
# Main migration orchestrator
# ---------------------------------------------------------------------------

def migrate(cfg: MigrationConfig):
    log.info("Connecting to source …")
    src_client = MongoClient(cfg.src_uri, readPreference=cfg.src_read_preference, serverSelectionTimeoutMS=10_000)
    src_db = src_client[cfg.src_db]
    src_col = src_db[cfg.collection]

    log.info("Connecting to destination …")
    dst_client = MongoClient(cfg.dst_uri, serverSelectionTimeoutMS=10_000)
    dst_db = dst_client[cfg.dst_db]
    dst_col = dst_db[cfg.dst_collection or cfg.collection]

    # Estimate total (for progress display)
    try:
        total = src_col.count_documents(cfg.src_query) if cfg.src_query else src_col.estimated_document_count()
        log.info("Source collection document count: %d", total)
    except Exception:
        total = None
        log.warning("Could not estimate document count.")

    # Copy indexes first (before data, so they build as data arrives)
    if cfg.copy_indexes:
        log.info("Replicating indexes …")
        replicate_indexes(src_col, dst_col, cfg.skip_id_index)

    checkpoint = Checkpoint(cfg.checkpoint_file)
    stats = Stats()
    stop_event = threading.Event()
    queue: Queue = Queue(maxsize=cfg.queue_depth)

    # Start progress reporter
    reporter = threading.Thread(
        target=progress_reporter,
        args=(stats, stop_event, total),
        daemon=True,
    )
    reporter.start()

    # Start reader
    reader = threading.Thread(
        target=reader_thread,
        args=(src_col, cfg, queue, checkpoint, stats, stop_event),
        daemon=True,
        name="reader",
    )
    reader.start()

    # Start writers
    log.info("Starting %d writer workers …", cfg.num_workers)
    writers = []
    for i in range(cfg.num_workers):
        w = threading.Thread(
            target=writer_thread,
            args=(dst_col, queue, checkpoint, stats, stop_event, cfg.upsert),
            daemon=True,
            name=f"writer-{i}",
        )
        w.start()
        writers.append(w)

    try:
        reader.join()
        for w in writers:
            w.join()
    except KeyboardInterrupt:
        log.warning("Interrupted — stopping workers (checkpoint saved).")
        stop_event.set()
        reader.join(timeout=5)
        for w in writers:
            w.join(timeout=5)

    stop_event.set()

    elapsed = time.monotonic()
    log.info(
        "Migration complete: %d inserted | %d skipped | %d errors",
        stats.inserted, stats.skipped, stats.errors,
    )

    if stats.errors == 0:
        checkpoint.clear()
        log.info("No errors — checkpoint cleared.")

    src_client.close()
    dst_client.close()

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Migrate a MongoDB collection between Atlas clusters.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--src-uri",       required=True,  help="Source MongoDB connection URI")
    p.add_argument("--dst-uri",       required=True,  help="Destination MongoDB connection URI")
    p.add_argument("--src-db",        required=True,  help="Source database name")
    p.add_argument("--dst-db",        required=True,  help="Destination database name (can be same)")
    p.add_argument("--collection",    required=True,  help="Source collection name to migrate")
    p.add_argument("--dst-collection", default=None, help="Destination collection name (defaults to same as --collection)")
    p.add_argument("--batch-size",    type=int, default=1000,  help="Documents per bulk insert")
    p.add_argument("--read-batch-size", type=int, default=5000, help="Cursor batch size from source")
    p.add_argument("--workers",       type=int, default=8,     help="Parallel insert threads")
    p.add_argument("--queue-depth",   type=int, default=50,    help="Max batches buffered in queue")
    p.add_argument("--checkpoint",    default=None,   help="Path to checkpoint file for resume support")
    p.add_argument("--upsert",        action="store_true",     help="Use upsert (idempotent, slower)")
    p.add_argument("--no-indexes",    action="store_true",     help="Skip index replication")
    p.add_argument("--query",         default="{}",            help="JSON filter to limit source docs, e.g. '{\"status\":\"active\"}'")
    p.add_argument("--read-preference", default="secondaryPreferred", help="Source read preference")
    return p.parse_args()


def main():
    args = parse_args()

    try:
        src_query = json.loads(args.query)
    except json.JSONDecodeError as exc:
        log.error("Invalid --query JSON: %s", exc)
        sys.exit(1)

    cfg = MigrationConfig(
        src_uri=args.src_uri,
        dst_uri=args.dst_uri,
        src_db=args.src_db,
        dst_db=args.dst_db,
        collection=args.collection,
        dst_collection=args.dst_collection,
        batch_size=args.batch_size,
        read_batch_size=args.read_batch_size,
        num_workers=args.workers,
        queue_depth=args.queue_depth,
        checkpoint_file=args.checkpoint,
        upsert=args.upsert,
        copy_indexes=not args.no_indexes,
        src_query=src_query,
        src_read_preference=args.read_preference,
    )

    stats = migrate(cfg)
    sys.exit(0 if stats.errors == 0 else 1)


if __name__ == "__main__":
    main()
