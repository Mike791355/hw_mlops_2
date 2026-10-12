import argparse
import csv
import json
import math
import os
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.request import Request, urlopen

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.serializer import Serializer


REQUIRED = {"transaction_time", "merch", "cat_id", "amount", "name_1", "name_2","gender", "street", "one_city", "us_state", "post_code", "lat", "lon","population_city", "jobs", "merchant_lat", "merchant_lon",}
FLOAT_FIELDS = ("lat", "lon", "merchant_lat", "merchant_lon")
CENT = Decimal("0.01")


def validate_paths(paths):
    paths = [Path(path) for path in paths]
    if not paths or len({path.name for path in paths}) != len(paths):
        raise ValueError("CSV names must be unique")
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(f"CSV not found: {path}")
    return paths


def iter_transactions(path, limit=None):
    path = Path(path)
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not REQUIRED.issubset(reader.fieldnames):
            missing = sorted(REQUIRED - set(reader.fieldnames or []))
            raise ValueError(f"{path}: missing columns: {missing}")
        for number, row in enumerate(reader, start=1):
            if limit is not None and number > limit:
                break
            try:
                if None in row or any(row[field] in (None, "") for field in REQUIRED):
                    raise ValueError("Invalid CSV row")
                amount = Decimal(row["amount"])
                if not amount.is_finite() or abs(amount) >= Decimal("10000000000"):
                    raise ValueError("amount out of range")
                if amount != amount.quantize(CENT):
                    raise ValueError("amount: max 2 decimals")
                timestamp = datetime.fromisoformat(row["transaction_time"])
                if timestamp.tzinfo is not None or timestamp.microsecond:
                    raise ValueError("Invalid timestamp format")
                if not datetime(1970, 1, 1) <= timestamp <= datetime(2106, 2, 7, 6, 28, 15):
                    raise ValueError("timestamp out of range")
                if row["gender"] not in ("F", "M"):
                    raise ValueError("Invalid gender")
                if (len(row["us_state"]) != 2 or not row["us_state"].isascii()
                        or not row["us_state"].isalpha() or not row["us_state"].isupper()):
                    raise ValueError("Invalid us_state")
                population = int(row["population_city"])
                if not 0 <= population <= 2**32 - 1:
                    raise ValueError("population_city out of range")
                target = row.get("target", "")
                if target not in ("", "0", "1"):
                    raise ValueError("Invalid target")
                result = {field: row[field] for field in REQUIRED}
                result.update(
                    source_file=path.name,
                    row_number=number,
                    transaction_time=timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    amount=format(amount, ".2f"),
                    population_city=population,
                    target=int(target) if target else None,
                )
                for field in FLOAT_FIELDS:
                    result[field] = float(row[field])
                    if not math.isfinite(result[field]):
                        raise ValueError(f"Invalid {field}")
                yield result
            except (ValueError, TypeError, InvalidOperation) as error:
                raise ValueError(f"{path}:{number}: {error}") from error


TABLES = ("transactions_baseline", "transactions")


def table_count(url, table):
    query = f"SELECT count() FROM homework.{table}"
    request = Request(url.rstrip("/") + "/", data=query.encode(), headers={
        "X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "homework"),
        "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", "homework"),
    })
    with urlopen(request, timeout=60) as response:
        return int(response.read())


def require_empty_topic(bootstrap_servers, topic):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers.split(","), group_id=None, enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Topic not found: {topic!r}")
        offsets = consumer.end_offsets([TopicPartition(topic, partition) for partition in partitions])
        if any(offsets.values()):
            raise RuntimeError("Topic not empty")
    finally:
        consumer.close()


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def positive_int(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError("Must be > 0")
    return value


def publish(paths, bootstrap_servers, topic="transactions", limit=None):
    paths = validate_paths(paths)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=JsonSerializer(),
        acks="all",
        enable_idempotence=True,
        retries=10,
        max_in_flight_requests_per_connection=5,
        compression_type="gzip",
        batch_size=131072,
        linger_ms=20,
        max_block_ms=60000,
    )
    sent = 0
    pending = []
    started = time.monotonic()
    try:
        for path in paths:
            for row in iter_transactions(path, limit):
                key = f"{path.name}:{row['row_number']}".encode("utf-8")
                pending.append(producer.send(topic, key=key, value=row))
                if len(pending) >= 10000:
                    for future in pending:
                        future.get(timeout=120)
                    sent += len(pending)
                    pending.clear()
                    if sent % 100000 == 0:
                        print(f"Kafka: {sent:,} ({time.monotonic() - started:.1f}s)", flush=True)
        for future in pending:
            future.get(timeout=120)
        sent += len(pending)
        producer.flush(timeout=120)
    finally:
        producer.close(timeout=30)
    print(f"Kafka: {sent:,}", flush=True)
    return sent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", nargs="+", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--limit", type=positive_int)
    args = parser.parse_args()
    paths = validate_paths(args.csv)
    require_empty_topic(args.bootstrap_servers, args.topic)
    clickhouse_url = os.getenv("CLICKHOUSE_URL")
    if clickhouse_url and any(table_count(clickhouse_url, table) for table in TABLES):
        raise RuntimeError("ClickHouse not empty")
    expected = 0
    for path in paths:
        count = sum(1 for _ in iter_transactions(path, args.limit))
        if not count:
            raise ValueError(f"{path}: empty CSV")
        expected += count
        print(f"{path.name}: {count:,}", flush=True)
    sent = publish(paths, args.bootstrap_servers, args.topic, args.limit)
    if sent != expected:
        raise RuntimeError(f"Sent {sent}, expected {expected}")
    if clickhouse_url:
        deadline = time.monotonic() + 900
        while True:
            counts = [table_count(clickhouse_url, table) for table in TABLES]
            if any(count > expected for count in counts):
                raise RuntimeError("ClickHouse duplicates")
            if all(count == expected for count in counts):
                print(f"ClickHouse: {expected:,}", flush=True)
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("ClickHouse timeout")
            time.sleep(2)


if __name__ == "__main__":
    main()

