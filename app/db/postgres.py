import psycopg2
from psycopg2.extras import RealDictCursor
from app.config import PG_CONFIG


def get_pg_connection():
    return psycopg2.connect(
        host=PG_CONFIG["host"],
        port=PG_CONFIG["port"],
        dbname=PG_CONFIG["dbname"],
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        cursor_factory=RealDictCursor,
    )


def test_pg_connection():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            return cur.fetchone()["ok"] == 1
    finally:
        conn.close()