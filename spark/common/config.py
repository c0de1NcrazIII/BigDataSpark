import os

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "petshop")
PG_USER = os.getenv("PG_USER", "petshop")
PG_PASSWORD = os.getenv("PG_PASSWORD", "petshop")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DB = os.getenv("CH_DB", "petshop")
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse")

def pg_jdbc_url() -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


def ch_jdbc_url() -> str:
    return f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"


def pg_properties() -> dict:
    return {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}


def ch_properties() -> dict:
    return {
        "user": CH_USER,
        "password": CH_PASSWORD,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }
