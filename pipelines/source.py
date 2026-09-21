from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from pipelines.config import Settings

@contextmanager
def database(settings: Settings) -> Iterator[psycopg2.extensions.connection]:
    connection = psycopg2.connect(settings.source_dsn)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

def claim_cdc_batch(settings: Settings) -> dict:
    """Persist all currently visible pending events as one immutable batch."""
    with database(settings) as connection:
        connection.set_session(isolation_level="REPEATABLE READ")
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext('retail_lakehouse_consumer'))")
            cursor.execute("SELECT batch_id,source_epoch,state,event_ids FROM control.batches WHERE state <> 'PUBLISHED' ORDER BY created_at LIMIT 1 FOR UPDATE")
            existing = cursor.fetchone()
            if existing:
                if existing["state"] == "FAILED":
                    cursor.execute("UPDATE control.batches SET state='CLAIMED', attempts=attempts+1, updated_at=now() WHERE batch_id=%s", (existing["batch_id"],))
                    existing["state"] = "CLAIMED"
                return dict(existing)
            cursor.execute("SELECT source_epoch FROM control.dataset_loads ORDER BY loaded_at DESC LIMIT 1")
            loaded = cursor.fetchone()
            if not loaded:
                raise RuntimeError("No source dataset has been loaded")
            cursor.execute("SELECT event_id FROM control.change_events WHERE source_epoch=%s AND acknowledged_at IS NULL ORDER BY event_id", (loaded["source_epoch"],))
            ids = [row["event_id"] for row in cursor.fetchall()]
            if not ids:
                return {"batch_id": None, "source_epoch": str(loaded["source_epoch"]), "state": "EMPTY", "event_ids": []}
            batch = uuid.uuid4()
            cursor.execute("INSERT INTO control.batches(batch_id,source_epoch,input_mode,state,event_ids,reconciliation) VALUES (%s,%s,'cdc','CLAIMED',%s,%s)", (batch, loaded["source_epoch"], ids, Json({"event_count":len(ids), "claimed_at":datetime.now(timezone.utc).isoformat()})))
            return {"batch_id": str(batch), "source_epoch": str(loaded["source_epoch"]), "state": "CLAIMED", "event_ids": ids}

def events_for_batch(settings: Settings, batch_id: str) -> list[dict]:
    with database(settings) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT event_ids FROM control.batches WHERE batch_id=%s FOR UPDATE", (batch_id,))
            batch = cursor.fetchone()
            if not batch:
                raise KeyError(f"Unknown batch: {batch_id}")
            cursor.execute("SELECT event_id,source_epoch,transaction_id,table_name,business_key,operation,record_before,record_after,occurred_at FROM control.change_events WHERE event_id=ANY(%s) ORDER BY event_id", (batch["event_ids"],))
            return [dict(row) for row in cursor.fetchall()]

def set_batch_state(settings: Settings, batch_id: str, state: str, detail: dict | None = None) -> None:
    if state not in {"LANDED","BRONZE_APPLIED","BRONZE_VALIDATED","TRANSFORMED","PUBLISHED","FAILED"}:
        raise ValueError(f"Invalid batch state: {state}")
    with database(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE control.batches SET state=%s, reconciliation=COALESCE(%s::jsonb,reconciliation), updated_at=now(), failure_stage=CASE WHEN %s='FAILED' THEN state ELSE NULL END, failure_detail=CASE WHEN %s='FAILED' THEN %s ELSE NULL END WHERE batch_id=%s", (state,json.dumps(detail) if detail else None,state,state,json.dumps(detail) if detail else None,batch_id))
            if cursor.rowcount != 1:
                raise KeyError(f"Unknown batch: {batch_id}")

def acknowledge_batch(settings: Settings, batch_id: str) -> None:
    with database(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT event_ids FROM control.batches WHERE batch_id=%s FOR UPDATE", (batch_id,))
            row = cursor.fetchone()
            if not row:
                raise KeyError(f"Unknown batch: {batch_id}")
            cursor.execute("UPDATE control.change_events SET acknowledged_at=now() WHERE event_id=ANY(%s) AND acknowledged_at IS NULL", (row[0],))

def record_source_check(settings: Settings, status: str, detail: dict) -> None:
    with database(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO control.source_health_checks(input_mode,status,detail) VALUES (%s,%s,%s)", (settings.input_mode,status,Json(detail)))
