-- Итоговая DDL: компактная таблица, агрегатная витрина и загрузка из Kafka.
-- Выполняется после 00_baseline.sql. Все представления создаются ДО producer.
CREATE DATABASE IF NOT EXISTS homework;

CREATE TABLE IF NOT EXISTS homework.transactions
(
    source_file LowCardinality(String) CODEC(ZSTD(1)),
    row_number UInt64 CODEC(Delta, ZSTD(1)),
    transaction_time DateTime('UTC') CODEC(Delta, ZSTD(1)),
    merch LowCardinality(String) CODEC(ZSTD(1)),
    cat_id LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal(12, 2) CODEC(ZSTD(1)),
    name_1 String CODEC(ZSTD(1)),
    name_2 String CODEC(ZSTD(1)),
    gender Enum8('F' = 1, 'M' = 2),
    street String CODEC(ZSTD(1)),
    one_city LowCardinality(String) CODEC(ZSTD(1)),
    us_state LowCardinality(String) CODEC(ZSTD(1)),
    post_code String CODEC(ZSTD(1)),
    lat Float64 CODEC(Gorilla, ZSTD(1)),
    lon Float64 CODEC(Gorilla, ZSTD(1)),
    population_city UInt32 CODEC(ZSTD(1)),
    jobs LowCardinality(String) CODEC(ZSTD(1)),
    merchant_lat Float64 CODEC(Gorilla, ZSTD(1)),
    merchant_lon Float64 CODEC(Gorilla, ZSTD(1)),
    target Nullable(UInt8) CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, transaction_time, source_file, row_number);

-- Состояние максимума на штат; категория и сумма принадлежат одной строке.
CREATE TABLE IF NOT EXISTS homework.state_max
(
    us_state String,
    winner AggregateFunction(
        argMax,
        Tuple(String, Decimal(12, 2)),
        Tuple(Decimal(12, 2), String, UInt64)
    )
)
ENGINE = AggregatingMergeTree
ORDER BY us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.transactions_to_state_max
TO homework.state_max
AS SELECT
    toString(us_state) AS us_state,
    argMaxState(
        tuple(toString(cat_id), amount),
        tuple(amount, toString(source_file), row_number)
    ) AS winner
FROM homework.transactions
GROUP BY us_state;

CREATE TABLE IF NOT EXISTS homework.transactions_kafka
(
    source_file String,
    row_number UInt64,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id String,
    amount Decimal(12, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target Nullable(UInt8)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse-homework-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 65536,
    kafka_flush_interval_ms = 2000,
    kafka_skip_broken_messages = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.kafka_to_baseline
TO homework.transactions_baseline
AS SELECT * FROM homework.transactions_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.kafka_to_transactions
TO homework.transactions
AS SELECT * FROM homework.transactions_kafka;

