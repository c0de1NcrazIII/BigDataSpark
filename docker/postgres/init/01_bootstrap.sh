#!/bin/bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SET client_encoding TO 'UTF8';
EOSQL

run_sql() {
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$1"
}

run_sql /sql/ddl/01_staging.sql
run_sql /sql/ddl/02_dimensions.sql
run_sql /sql/ddl/03_facts.sql

COPY_COLUMNS="id,customer_first_name,customer_last_name,customer_age,customer_email,customer_country,customer_postal_code,customer_pet_type,customer_pet_name,customer_pet_breed,seller_first_name,seller_last_name,seller_email,seller_country,seller_postal_code,product_name,product_category,product_price,product_quantity,sale_date,sale_customer_id,sale_seller_id,sale_product_id,sale_quantity,sale_total_price,store_name,store_location,store_city,store_state,store_country,store_phone,store_email,pet_category,product_weight,product_color,product_size,product_brand,product_material,product_description,product_rating,product_reviews,product_release_date,product_expiry_date,supplier_name,supplier_contact,supplier_email,supplier_phone,supplier_address,supplier_city,supplier_country"

shopt -s nullglob
files=(/data/csv/*.csv)
if [ "${#files[@]}" -eq 0 ]; then
    echo "CSV files not found in /data/csv" >&2
    exit 1
fi

for csv_file in "${files[@]}"; do
    echo "Loading ${csv_file}..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "\\copy mock_data (${COPY_COLUMNS}) FROM '${csv_file}' WITH (FORMAT csv, HEADER true, QUOTE '\"', ESCAPE '\"')"
done

echo "Staging load completed (DWH fill — Spark job 01_load_dwh)."
