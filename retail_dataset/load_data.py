"""Atomically load the owner-supplied CSV snapshot into PostgreSQL."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MIGRATIONS = ROOT.parent / "infra" / "postgres" / "migrations"
TABLES = (("customers", "customer_id"), ("stores", "store_id"), ("products", "product_id"), ("employees", "employee_id"), ("orders", "order_id"), ("order_items", "order_item_id"))


def fingerprint(data_dir: Path) -> tuple[str, dict[str, int]]:
    digest = hashlib.sha256()
    counts: dict[str, int] = {}
    for table, key in TABLES:
        path = data_dir / f"{table}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Required input does not exist: {path}")
        raw = path.read_bytes()
        lines = raw.decode("utf-8-sig").splitlines()
        if len(lines) < 2 or key not in lines[0].split(","):
            raise ValueError(f"Invalid CSV contract: {path}")
        digest.update(table.encode() + b"\0" + raw)
        counts[table] = len(lines) - 1
    return digest.hexdigest(), counts


def apply_migrations(cursor: psycopg2.extensions.cursor) -> None:
    for migration in sorted(MIGRATIONS.glob("*.sql")):
        cursor.execute("SELECT 1 FROM control.schema_migrations WHERE migration_id = %s", (migration.name,))
        if not cursor.fetchone():
            cursor.execute(migration.read_text(encoding="utf-8"))
            cursor.execute("INSERT INTO control.schema_migrations (migration_id) VALUES (%s)", (migration.name,))


def load(data_dir: Path, dsn: str) -> None:
    dataset_hash, counts = fingerprint(data_dir)
    with psycopg2.connect(dsn) as connection:
        with connection.cursor() as cursor:
            apply_migrations(cursor)
            cursor.execute("SELECT source_epoch FROM control.dataset_loads WHERE dataset_hash = %s", (dataset_hash,))
            existing = cursor.fetchone()
            if existing:
                print(json.dumps({"status": "noop", "dataset_hash": dataset_hash, "source_epoch": str(existing[0])}))
                return
            cursor.execute("SELECT EXISTS (SELECT 1 FROM raw.customers)")
            if cursor.fetchone()[0]:
                raise RuntimeError("A different source snapshot is already loaded; use a controlled migration, never truncate it.")
            epoch = uuid.uuid4()
            cursor.execute("INSERT INTO control.dataset_loads(dataset_hash,source_epoch,table_counts) VALUES (%s,%s,%s::jsonb)", (dataset_hash, str(epoch), json.dumps(counts)))
            for table, _ in TABLES:
                with (data_dir / f"{table}.csv").open("r", encoding="utf-8-sig", newline="") as stream:
                    query = sql.SQL("COPY raw.{} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')").format(sql.Identifier(table)).as_string(connection)
                    cursor.copy_expert(query, stream)
                cursor.execute(sql.SQL("SELECT count(*) FROM raw.{}").format(sql.Identifier(table)))
                if cursor.fetchone()[0] != counts[table]:
                    raise RuntimeError(f"Unexpected row count after loading {table}")
            cursor.execute("SELECT count(*) FROM control.change_events WHERE source_epoch=%s", (str(epoch),))
            events = cursor.fetchone()[0]
    print(json.dumps({"status": "loaded", "dataset_hash": dataset_hash, "source_epoch": str(epoch), "row_counts": counts, "change_events": events}))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.getenv("RETAIL_SOURCE_DSN"))
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or RETAIL_SOURCE_DSN is required")
    try:
        load(args.data_dir.resolve(), args.dsn)
    except Exception as error:
        print(f"load failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
