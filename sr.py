"""Thin StarRocks helpers over the MySQL protocol, shared by CI, tests and the exporter.

StarRocks speaks the MySQL wire protocol on port 9030, so pymysql is the whole client.
Stream Load is separate -- that is HTTP on 8030, and lives in ingester.py.
"""

import os
import sys
import time

import pymysql

HOST = os.getenv("STARROCKS_HOST", "127.0.0.1")
PORT = int(os.getenv("STARROCKS_PORT", "9030"))
USER = os.getenv("STARROCKS_USER", "root")
PASSWORD = os.getenv("STARROCKS_PASSWORD", "")


def connect(db=None):
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, database=db,
        cursorclass=pymysql.cursors.DictCursor, autocommit=True,
        # Pin the session clock. Writers and readers inheriting different timezone
        # defaults is how a freshness metric ends up negative.
        init_command="SET time_zone = '+00:00'",
    )


def query(sql, db="tapewatch"):
    with connect(db) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def wait_ready(timeout=420, interval=5):
    """Block until the FE answers AND a backend is alive.

    The FE accepts connections well before any BE has registered, so checking only
    'SELECT 1' gets you a cluster that authenticates and then fails every write.
    """
    deadline = time.monotonic() + timeout
    last = "no attempt made"
    while time.monotonic() < deadline:
        try:
            with connect() as conn, conn.cursor() as cur:
                cur.execute("SHOW BACKENDS")
                backends = cur.fetchall()
            alive = [b for b in backends if str(b.get("Alive", "")).lower() == "true"]
            if alive:
                return True
            last = f"{len(backends)} backend(s), none alive yet"
        except Exception as exc:  # noqa: BLE001 - still booting is the normal case
            last = f"{type(exc).__name__}: {exc}"
        time.sleep(interval)
    raise TimeoutError(f"StarRocks not ready after {timeout}s: {last}")


def apply_sql_file(path):
    """Execute a .sql file statement by statement.

    ponytail: splits on ';', so a semicolon inside a string literal would break it.
    Our DDL has none. Use a real parser if this ever loads arbitrary SQL.
    """
    text = open(path).read()
    statements = [s.strip() for s in text.split(";") if s.strip()]
    with connect() as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    return len(statements)


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "wait"
    if command == "wait":
        wait_ready()
        print(f"StarRocks ready at {HOST}:{PORT}")
    elif command == "apply":
        print(f"applied {apply_sql_file(sys.argv[2])} statements from {sys.argv[2]}")
    elif command == "query":
        for row in query(sys.argv[2]):
            print(row)
    else:
        raise SystemExit(f"unknown command: {command}")
