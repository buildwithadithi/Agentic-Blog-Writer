import os

from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from blog_writer.serialization import create_checkpoint_serializer

load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")

if not DB_URI:
    raise ValueError("SUPABASE_DB_URI is missing")


async def initialize_checkpointer():
    connection_pool = AsyncConnectionPool(
        conninfo=DB_URI,
        min_size=1,
        max_size=5,
        kwargs={
            "autocommit": True,
            "row_factory": dict_row,
        },
        open=False,
    )

    await connection_pool.open()

    checkpointer = AsyncPostgresSaver(
        connection_pool,
        serde=create_checkpoint_serializer(),
    )
    await checkpointer.setup()

    return connection_pool, checkpointer