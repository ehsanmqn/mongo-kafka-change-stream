#!/usr/bin/env python3
"""
mongo_to_kafka_async.py

Async MongoDB Change Stream → Kafka streamer.
Each Mongo collection writes to its own Kafka topic.

This variant adds robust handling for non-resumable change-stream errors:
if the stored resume token can no longer be used (oplog expired),
the watcher will log, remove the broken token, and open a fresh stream.
"""

import os
import asyncio
import json
import logging
import signal
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient
from aiokafka import AIOKafkaProducer
from bson import json_util
import pymongo.errors

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("mongo-to-kafka")

# ---------------------------
# Configuration (env)
# ---------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:password@mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "mydb")
MONGO_COLLECTIONS = [
    c.strip() for c in os.getenv("MONGO_COLLECTIONS", "collection1,collection2").split(",")
]

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "10.15.90.235:9092,10.15.90.235:9094,10.15.90.235:9096"
).split(",")

# if per-collection topic naming is not explicit, use this prefix:
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "mongo_")

QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "10000"))
BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE", "500"))
BATCH_MAX_DELAY_MS = int(os.getenv("BATCH_MAX_DELAY_MS", "200"))

RESUME_STORE_PATH = Path(os.getenv("RESUME_STORE_PATH", "/data/resume_tokens.json"))

KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BROKERS,
    "acks": "all",
    "linger_ms": int(os.getenv("KAFKA_LINGER_MS", "100")),
}


# ---------------------------
# Resume token persistence
# ---------------------------
def load_resume_tokens() -> Dict[str, Any]:
    if not RESUME_STORE_PATH.exists():
        return {}
    try:
        with RESUME_STORE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded resume tokens for {len(data)} collections")
        return data
    except Exception as e:
        logger.exception(f"Failed to load resume tokens: {e}")
        return {}


def save_resume_tokens_atomic(tokens: Dict[str, Any]) -> None:
    try:
        RESUME_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8",
                                         dir=str(RESUME_STORE_PATH.parent)) as tf:
            json.dump(tokens, tf, default=str)
            tmpname = tf.name
        Path(tmpname).replace(RESUME_STORE_PATH)
    except Exception:
        logger.exception("Failed to save resume tokens")


def delete_resume_token(tokens: Dict[str, Any], collection_name: str) -> None:
    """Remove a stored resume token for a collection and persist the file."""
    if collection_name in tokens:
        logger.warning(f"Removing stored resume token for collection '{collection_name}' "
                       "because it is no longer resumable.")
        tokens.pop(collection_name, None)
        save_resume_tokens_atomic(tokens)


# ---------------------------
# Utility: convert change doc -> bytes
# ---------------------------
def change_to_bytes(change: Dict[str, Any]) -> bytes:
    change["_streamed_at"] = datetime.now(timezone.utc).isoformat()
    return json_util.dumps(change).encode("utf-8")


# ---------------------------
# Kafka batch sender
# Handles (topic, msg_bytes) tuples from queue
# ---------------------------
async def kafka_batch_sender(producer: AIOKafkaProducer,
                             queue: asyncio.Queue[Tuple[str, bytes]],
                             stop_event: asyncio.Event):
    """
    Consume (topic, msg_bytes) from queue, batch per topic, and send.
    """
    batches: Dict[str, List[bytes]] = {}
    last_send = asyncio.get_event_loop().time()

    async def flush_batches():
        nonlocal batches, last_send
        if not batches:
            return
        for topic, msgs in list(batches.items()):
            if not msgs:
                continue
            for msg_bytes in msgs:
                await producer.send_and_wait(topic, msg_bytes)
            logger.info(f"Flushed {len(msgs)} messages to Kafka topic {topic}")
        await producer.flush()
        batches.clear()
        last_send = asyncio.get_event_loop().time()

    while not (stop_event.is_set() and queue.empty()):
        try:
            wait_timeout = BATCH_MAX_DELAY_MS / 1000.0
            topic, msg_bytes = await asyncio.wait_for(queue.get(), timeout=wait_timeout)
            queue.task_done()

            batch = batches.setdefault(topic, [])
            batch.append(msg_bytes)

            # flush if batch full
            if len(batch) >= BATCH_MAX_SIZE:
                await flush_batches()
        except asyncio.TimeoutError:
            # flush periodically
            if batches:
                await flush_batches()
            continue
        except Exception:
            logger.exception("Error in kafka_batch_sender loop")

    # final flush
    if batches:
        await flush_batches()
    logger.info("Kafka batch sender stopped")


# ---------------------------
# Watch a single Mongo collection (robust resume handling)
# ---------------------------
async def watch_collection(mongo_client: AsyncIOMotorClient,
                           db_name: str,
                           collection_name: str,
                           queue: asyncio.Queue,
                           resume_tokens: Dict[str, Any],
                           stop_event: asyncio.Event):
    col = mongo_client[db_name][collection_name]
    logger.info(f"Starting watcher for collection: {collection_name}")

    kafka_topic = f"{KAFKA_TOPIC_PREFIX}{collection_name}"

    # Helper to build watch kwargs from stored token if present
    def build_watch_kwargs(token_json: str | None) -> Dict[str, Any]:
        kwargs = {"full_document": "updateLookup"}
        if token_json:
            try:
                token_obj = json.loads(token_json)
                kwargs["resume_after"] = token_obj
            except Exception:
                logger.warning("Failed to parse stored resume token; starting fresh")
        return kwargs

    # outer loop: keep trying to (re)open the change stream until stop_event
    while not stop_event.is_set():
        resume_token_json = resume_tokens.get(collection_name)
        watch_kwargs = build_watch_kwargs(resume_token_json)
        try:
            # Try to open a change stream (may raise OperationFailure if resume token is gone)
            async with col.watch(**watch_kwargs) as stream:
                logger.info(
                    f"Opened change stream for {collection_name} (resume={'yes' if 'resume_after' in watch_kwargs else 'no'})")
                async for change in stream:
                    # Put the message on the kafka queue
                    payload = change_to_bytes({
                        "collection": collection_name,
                        **change
                    })
                    await queue.put((kafka_topic, payload))

                    # Persist resume token (store as JSON string)
                    try:
                        rt = change.get("_id")
                        if rt is not None:
                            resume_tokens[collection_name] = json.dumps(rt, default=str)
                            save_resume_tokens_atomic(resume_tokens)
                    except Exception:
                        logger.exception("Failed to persist resume token")

                    if stop_event.is_set():
                        break

        except pymongo.errors.OperationFailure as op_err:
            # Check for non-resumable error (oplog trimmed, etc.)
            try:
                errinfo = getattr(op_err, "details", None) or {}
                code = errinfo.get("code", None)
                codeName = errinfo.get("codeName", "")
                labels = errinfo.get("errorLabels", []) or []
            except Exception:
                code = None
                codeName = ""
                labels = []

            # Common marker: ChangeStreamHistoryLost / NonResumableChangeStreamError
            non_resumable = False
            if code == 286 or "ChangeStreamHistoryLost" in codeName or "NonResumableChangeStreamError" in labels:
                non_resumable = True

            logger.warning(
                f"Change stream OperationFailure on {collection_name}: {op_err}; non_resumable={non_resumable}")

            if non_resumable:
                # remove stored resume token and start fresh
                delete_resume_token(resume_tokens, collection_name)
                # small backoff before restarting fresh stream
                await asyncio.sleep(1)
                # set watch_kwargs to fresh (no resume) and continue outer loop
                continue
            else:
                # other OperationFailure -> backoff and retry
                await asyncio.sleep(2)
                continue

        except Exception as e:
            # Unexpected exception while opening/iterating the stream
            logger.exception(f"Unexpected error in change stream for {collection_name}: {e}")
            # Backoff a bit before retrying to avoid tight loop
            await asyncio.sleep(2)
            continue

        # If we exit the 'async with' normally (e.g., stream closed), small sleep then retry
        await asyncio.sleep(0.5)

    logger.info(f"Watcher stopped for collection: {collection_name}")


# ---------------------------
# Main
# ---------------------------
async def main():
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    loop.add_signal_handler(signal.SIGINT, _signal_handler)

    resume_tokens = load_resume_tokens()
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    producer = AIOKafkaProducer(**KAFKA_PRODUCER_CONFIG)
    await producer.start()
    logger.info("Kafka producer started")

    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    sender_task = asyncio.create_task(kafka_batch_sender(producer, queue, stop_event))

    watchers = [
        asyncio.create_task(
            watch_collection(mongo_client, MONGO_DB, c, queue, resume_tokens, stop_event)
        )
        for c in MONGO_COLLECTIONS if c
    ]

    logger.info(f"Started watchers for: {MONGO_COLLECTIONS}")
    await stop_event.wait()

    logger.info("Stopping watchers...")
    for w in watchers:
        w.cancel()

    await asyncio.gather(*watchers, return_exceptions=True)
    await queue.join()

    stop_event.set()
    await sender_task

    await producer.stop()
    mongo_client.close()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Fatal error in main")
