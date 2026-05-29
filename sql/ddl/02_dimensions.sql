-- Модель «снежинка»: нормализованные измерения

CREATE TABLE dim_country (
    country_sk   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_city (
    city_sk      SERIAL PRIMARY KEY,
    city_name    TEXT NOT NULL,
    state_name   TEXT,
    country_sk   INTEGER NOT NULL REFERENCES dim_country (country_sk),
    UNIQUE (city_name, state_name, country_sk)
);

CREATE TABLE dim_product_category (
    category_sk      SERIAL PRIMARY KEY,
    product_category TEXT NOT NULL,
    pet_category     TEXT,
    UNIQUE (product_category, pet_category)
);

CREATE TABLE dim_date (
    date_sk    SERIAL PRIMARY KEY,
    full_date  DATE NOT NULL UNIQUE,
    year_num   SMALLINT NOT NULL,
    month_num  SMALLINT NOT NULL,
    day_num    SMALLINT NOT NULL,
    quarter_num SMALLINT NOT NULL
);

CREATE TABLE dim_customer (
    customer_sk         SERIAL PRIMARY KEY,
    source_customer_id  INTEGER,
    first_name          TEXT,
    last_name           TEXT,
    age                 INTEGER,
    email               TEXT NOT NULL UNIQUE,
    postal_code         TEXT,
    pet_type            TEXT,
    pet_name            TEXT,
    pet_breed           TEXT,
    country_sk          INTEGER REFERENCES dim_country (country_sk)
);

CREATE TABLE dim_seller (
    seller_sk         SERIAL PRIMARY KEY,
    source_seller_id  INTEGER,
    first_name        TEXT,
    last_name         TEXT,
    email             TEXT NOT NULL UNIQUE,
    postal_code       TEXT,
    country_sk        INTEGER REFERENCES dim_country (country_sk)
);

CREATE TABLE dim_product (
    product_sk          SERIAL PRIMARY KEY,
    source_product_id   INTEGER,
    product_name        TEXT,
    category_sk         INTEGER NOT NULL REFERENCES dim_product_category (category_sk),
    list_price          NUMERIC(12, 2),
    stock_quantity      INTEGER,
    weight              NUMERIC(12, 2),
    color               TEXT,
    size_label          TEXT,
    brand               TEXT,
    material            TEXT,
    description         TEXT,
    rating              NUMERIC(4, 2),
    reviews_count       INTEGER,
    release_date        DATE,
    expiry_date         DATE,
    UNIQUE (
        source_product_id,
        product_name,
        category_sk,
        list_price,
        brand,
        material,
        color,
        size_label
    )
);

CREATE TABLE dim_store (
    store_sk        SERIAL PRIMARY KEY,
    store_name      TEXT NOT NULL,
    store_location  TEXT,
    city_sk         INTEGER NOT NULL REFERENCES dim_city (city_sk),
    phone           TEXT,
    email           TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_supplier (
    supplier_sk   SERIAL PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    contact_name  TEXT,
    email         TEXT NOT NULL UNIQUE,
    phone         TEXT,
    address       TEXT,
    city_sk       INTEGER REFERENCES dim_city (city_sk),
    country_sk    INTEGER REFERENCES dim_country (country_sk)
);
