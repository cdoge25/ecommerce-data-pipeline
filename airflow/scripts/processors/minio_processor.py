import os

import duckdb

from airflow.hooks.base import BaseHook

PROCESSED_BUCKET = "processed"
MINIO_ACCESS_KEY_ID = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_ACCESS_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")


class MinioCSVFileProcessor:
    def __init__(self):
        self.conn = duckdb.connect()
        # self.airflow_minio_connection = BaseHook.get_connection("minio")
        # self.conn.execute(f"""
        #     SET s3_access_key_id = '{self.airflow_minio_connection.login}';
        #     SET s3_secret_access_key = '{self.airflow_minio_connection.password}';
        #     SET s3_endpoint = \'{self.airflow_minio_connection.extra_dejson["endpoint_url"].split("//")[1]}\';
        #     SET s3_url_style = 'path';
        #     SET s3_use_ssl = false;
        # """)
        self.conn.execute(f"""
            SET s3_access_key_id = '{MINIO_ACCESS_KEY_ID}';
            SET s3_secret_access_key = '{MINIO_SECRET_ACCESS_KEY}';
            SET s3_endpoint = '{MINIO_ENDPOINT}';
            SET s3_url_style = 'path';
            SET s3_use_ssl = false;
        """)
        self.table_cleaning_rules = {
            "customers": self._process_customers,
            "order_items": self._process_order_items,
            "order_payments": self._process_order_payments,
            "order_reviews": self._process_order_reviews,
            "orders": self._process_orders,
            "product_category_name_translation": self._process_product_category_name_translation,
            "products": self._process_products,
            "sellers": self._process_sellers,
        }

    def _process_file(self, file_path):
        bucket, object_name = file_path.split("/", 1)
        table_name = object_name.split(".")[0]
        # No duplicates - applied for all files
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT DISTINCT * FROM read_csv_auto('s3://{bucket}/{object_name}')"
        )
        # Apply rules for specific files
        cleaning_function = self._get_cleaning_function(table_name)
        cleaning_function()
        self._save_file(table_name)
        processed_file_path = f"{PROCESSED_BUCKET}/{table_name}.parquet"
        return processed_file_path

    def _get_cleaning_function(self, table_name):
        return self.table_cleaning_rules.get(table_name)

    def _process_customers(self):
        self.conn.execute("""
            ALTER TABLE customers
                RENAME COLUMN customer_id TO customer_address_id;
            ALTER TABLE customers
                RENAME COLUMN customer_unique_id TO customer_id;
            DELETE FROM customers
                WHERE
                    customer_address_id IS NULL 
                    OR customer_address_id = ''
                    OR customer_id IS NULL
                    OR customer_id = '';
        """)

    def _process_order_items(self):
        self.conn.execute("""
            ALTER TABLE order_items
                RENAME COLUMN order_item_id TO order_product_sequence;
            ALTER TABLE order_items
                RENAME COLUMN shipping_limit_date TO order_limit_delivery_timestamp;
            DELETE FROM order_items
                WHERE
                    order_id IS NULL
                    OR order_id = '';
        """)

    def _process_order_payments(self):
        self.conn.execute("""
            ALTER TABLE order_payments
                RENAME COLUMN payment_sequential TO payment_sequence;
            DELETE FROM order_payments
                WHERE
                    order_id IS NULL
                    OR order_id = '';
        """)

    def _process_order_reviews(self):
        self.conn.execute("""
            DELETE FROM order_reviews
                WHERE
                    order_id IS NULL
                    OR order_id = ''
                    OR review_id IS NULL
                    OR review_id = '';
        """)

    def _process_orders(self):
        self.conn.execute("""
            ALTER TABLE orders
                RENAME COLUMN customer_id TO customer_address_id;
            ALTER TABLE orders
                RENAME COLUMN order_approved_at TO order_approved_timestamp;
            ALTER TABLE orders
                RENAME COLUMN order_delivered_carrier_date TO order_pickup_timestamp;
            ALTER TABLE orders
                RENAME COLUMN order_delivered_customer_date TO order_delivered_timestamp;
            DELETE FROM orders
                WHERE
                    order_id IS NULL
                    OR order_id = '';
        """)

    def _process_product_category_name_translation(self):
        self.conn.execute("""
            DELETE FROM product_category_name_translation
                WHERE
                    product_category_name IS NULL
                    OR product_category_name = '';
        """)

    def _process_products(self):
        self.conn.execute("""
            ALTER TABLE products
                RENAME COLUMN product_name_lenght TO product_name_length;
            ALTER TABLE products
                RENAME COLUMN product_description_lenght TO product_description_length;
            ALTER TABLE products
                RENAME COLUMN product_photos_qty TO product_photos_quantity;
            DELETE FROM products
                WHERE
                    product_id IS NULL
                    OR product_id = '';
        """)

    def _process_sellers(self):
        self.conn.execute("""
            DELETE FROM sellers
                WHERE
                    seller_id IS NULL
                    OR seller_id = '';
        """)

    def _save_file(self, table_name):
        self.conn.execute(f"""
            COPY {table_name}
            TO 's3://{PROCESSED_BUCKET}/{table_name}.parquet'
            (FORMAT 'parquet');
        """)
