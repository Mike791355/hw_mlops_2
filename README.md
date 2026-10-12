ДЗ 2

Запуск из корня проекта:

```bash
docker compose run --build --rm producer
```

Команда создаёт топик и таблицы, загружает данные и ждёт их появления в ClickHouse. Ожидаемый итог: ClickHouse: 1,048,575.

Проверка количества строк:

```bash
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --query "SELECT count() FROM homework.transactions"
```

Ожидается 1048575 строк. В test.csv нет target, он сохраняется как NULL.

Запрос и сохранение ответа:

```bash
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --multiquery < sql/03_max_transaction_by_state_optimized.sql > results/max_transaction_by_state.csv
```

Ответ содержит 51 строку, включая DC. Исходный запрос даёт тот же ответ:

```bash
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --multiquery < sql/02_max_transaction_by_state.sql
```

Проверка на демо вместо полного набора, на пустом стеке:

```bash
docker compose run --build --rm producer --csv /data/demo.csv
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --multiquery < sql/03_max_transaction_by_state_optimized.sql
```

Демо содержит 10 строк. 

Ответ: CA - travel - 100.20; DC - entertainment - 75.55; NY - shopping_pos - 50.00; TX - gas_transport - 100.01.

Основные файлы:

```text
scripts/producer.py - CSV в Kafka
sql/00_baseline.sql — исходная таблица
sql/01_schema.sql — оптимизированные таблицы и загрузка из Kafka
sql/02_max_transaction_by_state.sql — исходный запрос
sql/03_max_transaction_by_state_optimized.sql — запрос к витрине
results/max_transaction_by_state.csv — результат на полном наборе
```

us_state - штат, cat_id - категория, amount - сумма. При одинаковых суммах выбирается максимум (source_file, row_number).

Оптимизация: LowCardinality, компактные типы, кодеки, сортировка по штату и времени, месячные партиции. Витрина state_max хранит argMaxState по штату; запрос объединяет состояния через argMaxMerge.

Замер скорости: выполните первые две команды , затем повторите их семь раз. Последние две команды показывают результаты.

```bash
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --max_threads 2 --use_query_cache 0 --log_queries 1 --query_id homework-baseline --multiquery < sql/02_max_transaction_by_state.sql
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --max_threads 2 --use_query_cache 0 --log_queries 1 --query_id homework-optimized --multiquery < sql/03_max_transaction_by_state_optimized.sql
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --query "SYSTEM FLUSH LOGS"
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --query "SELECT query_id, query_duration_ms, read_rows, read_bytes FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ('homework-baseline', 'homework-optimized') ORDER BY event_time DESC LIMIT 14"
```

Проверка размера таблиц:

```bash
docker compose exec -T clickhouse clickhouse-client --user homework --password homework --query "SELECT table, sum(bytes_on_disk) AS bytes FROM system.parts WHERE active AND database = 'homework' GROUP BY table ORDER BY table"
```

Запуск загрузчика вне Docker: нужен Python 3.13+, данные и пустой стек.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d
docker compose run --rm schema-init
python scripts/producer.py --csv data/train.csv data/test.csv
```

Kafka доступна на localhost:19092.

Повторная загрузка требует очистки данных Kafka и ClickHouse. Эта команда удаляет данные контейнеров, исходные CSV сохраняются:

```bash
docker compose down --volumes --remove-orphans
```

Очистка нужна также при переходе с демо на полный набор и после прерванной загрузки. 

Остановка с сохранением данных и повторный запуск:

```bash
docker compose down
docker compose up -d
```

Проверка контейнеров и логов:

```bash
docker compose ps -a
docker compose logs --tail 100 kafka clickhouse kafka-init schema-init
```

