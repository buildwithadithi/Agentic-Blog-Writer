import os

from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from langgraph.checkpoint.postgres import PostgresSaver

load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")

if not DB_URI:
    raise ValueError("SUPABASE_DB_URI is missing")

# Create a connection pool
connection_pool = ConnectionPool(
    conninfo=DB_URI,
    min_size=1,
    max_size=5,
    kwargs={
        "autocommit": True,
        "row_factory": dict_row,
    },
)

# Create the checkpointer
checkpointer = PostgresSaver(connection_pool)

# Create checkpoint tables and apply migrations
checkpointer.setup()
try:
    # Wait until the connection pool establishes its connections
    connection_pool.wait()

    # Execute a test query
    with connection_pool.connection() as conn:
        
        result = conn.execute(
            "SELECT 1 AS test_value"
        ).fetchone()

        print(result)
        print("Database connection successful!")
        print("Test query result:", result)

finally:
    # Close the connection pool properly
    connection_pool.close()