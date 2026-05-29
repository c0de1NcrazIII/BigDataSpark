# BigDataSpark — лабораторная работа №2

ETL на **Apache Spark**: загрузка модели **«снежинка»** в PostgreSQL и построение **6 аналитических витрин** в **ClickHouse**.

## Структура репозитория

| Путь | Назначение |
|------|------------|
| `исходные данные/` | 10 CSV (`MOCK_DATA.csv` … `MOCK_DATA (9).csv`) |
| `sql/ddl/` | DDL staging и DWH в PostgreSQL |
| `sql/checks/` | Проверочные запросы PG и ClickHouse |
| `docker-compose.yml` | PostgreSQL, ClickHouse, Spark |
| `docker/postgres/init/` | Загрузка CSV в `mock_data` |
| `docker/clickhouse/init/` | База `petshop` и таблицы витрин |
| `spark/jobs/01_load_dwh.py` | Spark: `mock_data` → снежинка в PostgreSQL |
| `spark/jobs/02_build_clickhouse_marts.py` | Spark: 6 витрин в ClickHouse |
| `scripts/run_spark_jobs.sh` | Запуск обеих Spark-джоб |

## Витрины в ClickHouse (6 таблиц)

1. `mart_product_sales` — продукты и выручка по категориям  
2. `mart_customer_sales` — клиенты и распределение по странам  
3. `mart_time_sales` — помесячные и годовые тренды  
4. `mart_store_sales` — магазины  
5. `mart_supplier_sales` — поставщики  
6. `mart_product_quality` — рейтинги и отзывы  

## Запуск

Требуется Docker и Docker Compose.

```bash
docker compose up --build -d
```

Контейнер `spark` после старта один раз выполняет обе джобы и завершается.

При первом старте:

1. PostgreSQL создаёт таблицы и загружает 10 CSV в `mock_data` (10 000 строк).  
2. Контейнер `spark` выполняет джобы `01_load_dwh` и `02_build_clickhouse_marts`.

Порты (чтобы не конфликтовать с лабой №1):

| Сервис | Порт |
|--------|------|
| PostgreSQL | `5434` |
| ClickHouse HTTP | `8123` |
| ClickHouse native | `9000` |

### Проверка PostgreSQL

```bash
docker compose exec postgres psql -U petshop -d petshop -f /sql/checks/validation.sql
```

Ожидается: `mock_data` и `fact_sales` = **10 000**, `revenue_match = OK`.

### Проверка ClickHouse

```bash
docker compose exec clickhouse clickhouse-client --query "$(cat sql/checks/clickhouse_marts.sql)"
```

Или в DBeaver: `jdbc:clickhouse://localhost:8123/petshop`.

### Повторный запуск только Spark-джоб

```bash
docker compose run --rm spark
```

### Полный сброс

```bash
docker compose down -v
docker compose up
```

## Ручной spark-submit (без пересоздания контейнеров)

```bash
docker compose up -d postgres clickhouse
docker compose run --rm spark
```

## Подключения

| Сервис | Host | Port | User | Password | DB |
|--------|------|------|------|----------|-----|
| PostgreSQL | localhost | 5434 | petshop | petshop | petshop |
| ClickHouse | localhost | 8123 | default | clickhouse | petshop |

## Исходное задание

[MAIStudents/BigDataSpark](https://github.com/MAIStudents/BigDataSpark)
