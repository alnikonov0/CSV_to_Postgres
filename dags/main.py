import csv
import pandas as pd
import psycopg2
import io
import sys
import os

sys.path.append(os.path.dirname(__file__))

class ETL:
    def __init__(self):
        self.conn = None
        pass

    def connect(self):
        self.conn = psycopg2.connect(
            host="my_postgres",
            user="postgres",
            password="mysecretpassword",
            database="postgres",
            port="5432"
        )

    def extract(self, file_path, batch_size):
        with open(file_path) as f:
            batch = []
            reader = csv.DictReader(f)
            for row in reader:
                batch.append(row)
                if len(batch) == batch_size:
                    yield batch
                    batch.clear()

            if batch:
                yield batch

    def transform(self, batch):
        df = pd.DataFrame(batch)
        return df

    def load(self, df, dst_table):
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        with self.conn.cursor() as cur:
            if '.' in dst_table:
                # если есть схема — используем copy_expert
                cur.copy_expert(
                    f"COPY {dst_table} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT CSV, HEADER)",
                    buffer
                )
            else:
                # иначе обычный copy_from
                cur.copy_from(buffer, dst_table, sep=",", columns=df.columns)
            self.conn.commit()

    def run(self, file_path, batch_size, dst_table):
        conn = self.connect()
        for batch in self.extract(file_path, batch_size):
            transformed = self.transform(batch)
            self.load(transformed, dst_table)
        pass

# currency = ETL()
# # currency.connect()
# currency.run('test_etl_data_1m.csv', 100000, 'stg.clients')
