import psycopg2

# Database connection parameters for the default 'postgres' database
db_params = {
    'dbname': 'postgres',  # Connect to the default database
    'user': 'postgres',
    'password': 'Rohansb10',
    'host': 'localhost',
    'port': '5432',
    'schema': 'public',  # Optional, specify schema if needed
    'db_name' : 'postgres'  # Optional, specify database name if needed
}

# Connect to the default PostgreSQL database
conn = psycopg2.connect(**db_params)
conn.autocommit = True  # Set autocommit for database creation

# Create a cursor object
cursor = conn.cursor()

# SQL command to create a new database
database_name = 'items'
create_db_command = f"CREATE DATABASE {database_name};"

try:
    # Execute the command to create the database
    cursor.execute(create_db_command)
    print(f"Database '{database_name}' created successfully.")
except psycopg2.errors.DuplicateDatabase:
    print(f"Database '{database_name}' already exists.")
finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()
