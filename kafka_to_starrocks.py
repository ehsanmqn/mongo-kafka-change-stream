#!/usr/bin/env python3
"""
kafka_to_starrocks.py

Consumes MongoDB change stream messages from Kafka and writes them into StarRocks.
If a target table doesn't exist, it's created automatically.

Now optimized with batch inserts to avoid StarRocks 'too many versions' errors.
"""

import os
import asyncio
import logging
import aiokafka
import pymysql
import orjson
import time

from datetime import datetime
from pymysql.constants import CLIENT

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("kafka-to-starrocks")

# ---------------------------
# Env configuration
# ---------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mongo-collection1")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "starrocks-writer")

STARROCKS_HOST = os.getenv("STARROCKS_HOST", "localhost")
STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")
STARROCKS_PASS = os.getenv("STARROCKS_PASS", "")
STARROCKS_DB = os.getenv("STARROCKS_DB", "mongo_data")

# ---------------------------
# Batch configuration
# ---------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))  # how many rows before flush
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", "3"))  # seconds between forced flush


# ---------------------------
# StarRocks utilities
# ---------------------------

def get_starrocks_conn():
    return pymysql.connect(
        host=STARROCKS_HOST,
        port=STARROCKS_PORT,
        user=STARROCKS_USER,
        password=STARROCKS_PASS,
        database=STARROCKS_DB,
        client_flag=CLIENT.MULTI_STATEMENTS
    )


def normalize_mongo_row(row: dict) -> dict:
    """
    Normalize and flatten a MongoDB document for SQL/StarRocks compatibility:
    - Converts {"$date": "..."} → 'YYYY-MM-DD HH:MM:SS'
    - Converts {"$oid": "..."} → string
    - Serializes nested dicts/lists to JSON
    - Converts ISO-like datetime strings to SQL format
    - Truncates long string values (>255 chars)
    """
    MAX_LEN = 255

    def normalize_value(v):
        if isinstance(v, dict):
            # Handle Mongo special cases
            if "$date" in v:
                v = v["$date"]
                try:
                    cleaned = v.replace("T", " ").replace("Z", "").split("+")[0].split(".")[0]
                    dt = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
                    v = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
                return v[:MAX_LEN] if isinstance(v, str) and len(v) > MAX_LEN else v

            elif "$oid" in v:
                v = v["$oid"]
                return v[:MAX_LEN] if isinstance(v, str) and len(v) > MAX_LEN else v

            else:
                v = orjson.dumps(v).decode("utf-8")
                return v[:MAX_LEN] if len(v) > MAX_LEN else v

        elif isinstance(v, list):
            v = orjson.dumps(v).decode("utf-8")
            return v[:MAX_LEN] if len(v) > MAX_LEN else v

        elif isinstance(v, str):
            # Normalize ISO-like timestamps
            try:
                cleaned = v.replace("T", " ").replace("Z", "").split("+")[0].split(".")[0]
                dt = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
                v = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            return v[:MAX_LEN] if len(v) > MAX_LEN else v

        else:
            return v

    return {k: normalize_value(v) for k, v in row.items()}


def ensure_columns_exist(conn, table_name: str, row: dict):
    """Check if any columns in row are missing, add them dynamically."""
    cursor = conn.cursor()
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_cols = set(col[0] for col in cursor.fetchall())

    alter_statements = []
    for key, value in row.items():
        if key not in existing_cols:
            if isinstance(value, bool) or isinstance(value, int):
                coltype = "BIGINT"
            elif isinstance(value, float):
                coltype = "DOUBLE"
            elif isinstance(value, str) and len(value) > 255:
                coltype = "VARCHAR(4000)"
            elif isinstance(value, str):
                coltype = "VARCHAR(255)"
            else:
                coltype = "STRING"
            alter_statements.append(f"ADD COLUMN `{key}` {coltype} NULL")

    if alter_statements:
        alter_sql = f"ALTER TABLE `{table_name}` " + ", ".join(alter_statements)
        logger.info(f"Altering table `{table_name}` to add columns: {alter_statements}")
        cursor.execute(alter_sql)
        conn.commit()
    cursor.close()


def ensure_table_exists(conn, table_name: str, sample_row: dict):
    """Create the table in StarRocks if it does not exist, using AUTO ID as primary key."""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    if cursor.fetchone():
        cursor.close()
        return

    # Add auto-incrementing primary key
    columns = ["`id` BIGINT AUTO_INCREMENT"]

    for key, value in sample_row.items():
        if isinstance(value, bool) or isinstance(value, int):
            coltype = "BIGINT"
        elif isinstance(value, float):
            coltype = "DOUBLE"
        elif isinstance(value, str) and len(value) > 255:
            coltype = "VARCHAR(4000)"
        elif isinstance(value, str):
            coltype = "VARCHAR(255)"
        else:
            coltype = "STRING"
        columns.append(f"`{key}` {coltype}")

    cols_sql = ",\n  ".join(columns)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      {cols_sql}
    )
    PRIMARY KEY(`id`)
    DISTRIBUTED BY HASH(`id`)
    PROPERTIES("replication_num" = "2");
    """

    logger.info(f"Creating StarRocks table: {table_name}")
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()


def insert_many(conn, table_name: str, rows: list[dict]):
    """Insert a batch of rows efficiently and safely."""
    if not rows:
        return

    # Collect all unique columns across all rows (not just the first one)
    all_cols = set()
    for row in rows:
        all_cols.update(row.keys())
    cols = sorted(all_cols)  # sorted for consistent order

    col_sql = ", ".join(f"`{c}`" for c in cols)
    placeholders = "(" + ", ".join(["%s"] * len(cols)) + ")"
    sql = f"INSERT INTO `{table_name}` ({col_sql}) VALUES {placeholders}"

    # Safely handle missing keys
    values = [tuple(row.get(c) for c in cols) for row in rows]

    cursor = conn.cursor()
    try:
        cursor.executemany(sql, values)
        conn.commit()
        logger.info(f"Inserted {len(rows)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logger.exception(f"Batch insert failed for {table_name}: {e}")
        logger.error(f"SQL: {sql}")
        if values:
            logger.error(f"Sample row: {values[0]}")
    finally:
        cursor.close()


# ---------------------------
# Kafka consumer
# ---------------------------
async def consume_kafka():
    consumer = aiokafka.AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: orjson.loads(m.decode("utf-8"))
    )

    await consumer.start()
    logger.info(f"Listening to Kafka topic '{KAFKA_TOPIC}' ...")

    conn = get_starrocks_conn()
    batch = []
    last_flush = time.time()

    try:
        async for msg in consumer:
            data = msg.value
            if not isinstance(data, dict):
                logger.warning(f"Skipping invalid message: {data}")
                continue

            # Determine table name from collection
            collection = data.get("collection", "unknown")
            table_name = collection.replace("-", "_")

            # Use fullDocument if present, otherwise the entire message
            full_doc = data.get("fullDocument") or data

            # Ensure '_streamed_at' exists
            full_doc["_streamed_at"] = data.get("_streamed_at") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Flatten nested dicts/lists and normalize dates
            flat_doc = normalize_mongo_row(full_doc)

            # Convert MongoDB ObjectId JSON string to plain string
            if "_id" in flat_doc:
                _id_val = flat_doc["_id"]
                if isinstance(_id_val, str):
                    try:
                        parsed = orjson.loads(_id_val)
                        if isinstance(parsed, dict) and "$oid" in parsed:
                            flat_doc["_id"] = parsed["$oid"]
                    except Exception:
                        flat_doc["_id"] = str(_id_val)  # fallback to string

            # Remove unwanted fields
            for field in ["extendedInfo", "widgets", "extraData"]:
                flat_doc.pop(field, None)

            # Ensure table and columns exist before first insert
            if not batch:
                ensure_table_exists(conn, table_name, flat_doc)
                ensure_columns_exist(conn, table_name, flat_doc)

            batch.append(flat_doc)

            # Flush batch if conditions met
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) > BATCH_INTERVAL:
                insert_many(conn, table_name, batch)
                batch.clear()
                last_flush = time.time()

        # Flush any remaining records on exit
        if batch:
            insert_many(conn, table_name, batch)

    finally:
        await consumer.stop()
        conn.close()
        logger.info("Kafka consumer stopped and StarRocks connection closed.")


# ---------------------------
# Entrypoint
# ---------------------------
if __name__ == "__main__":
    try:
        asyncio.run(consume_kafka())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception:
        logger.exception("Fatal error in consumer.")
