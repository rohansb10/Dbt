import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Step 1: Load the CSV
csv_file_path = r"C:\Users\rohan\Dbt\my_dbt_project\seeds\data.csv"
df = pd.read_csv(csv_file_path)

# Step 2: Connect to Neon Postgres
conn = psycopg2.connect(
    dbname="neondb",
    user="neondb_owner",
    password="npg_lBOgysC19xQV",
    host="ep-rapid-thunder-a10g061l-pooler.ap-southeast-1.aws.neon.tech",
    sslmode="require"
)
cur = conn.cursor()

# Step 3: Create the target table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS items (
    item_id TEXT,
    item_type TEXT,
    item_mrp FLOAT,
    store_id TEXT
)
"""
cur.execute(create_table_query)
conn.commit()

# Step 4: Insert data
insert_query = """
INSERT INTO items (item_id, item_type, item_mrp, store_id) VALUES %s
"""
values = [tuple(x) for x in df.to_numpy()]
execute_values(cur, insert_query, values)

conn.commit()
cur.close()
conn.close()

print("✅ Data inserted successfully into Neon!")
