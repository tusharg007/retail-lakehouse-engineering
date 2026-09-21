"""Replay-safe local Spark/Delta bronze ingestion for CDC batches."""
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.config import Settings
from pipelines.source import acknowledge_batch, events_for_batch, set_batch_state

PRIMARY_KEYS = {"customers":"customer_id", "stores":"store_id", "products":"product_id", "employees":"employee_id", "orders":"order_id", "order_items":"order_item_id"}

def spark_session(root: str) -> SparkSession:
    return (SparkSession.builder.appName("retail-bronze")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", f"{root}/warehouse")
            .config("spark.jars", "/opt/airflow/delta-spark_2.12-3.2.0.jar,/opt/airflow/delta-storage-3.2.0.jar")
            .getOrCreate())

def landing_path(root: str, batch_id: str) -> Path:
    return Path(root) / "landing" / f"batch_id={batch_id}"

def land_events(settings: Settings, batch_id: str) -> Path:
    target = landing_path(settings.lakehouse_root, batch_id)
    manifest_path = target / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("format_version") == 2:
            return target
        # This is a failed, pre-format landing attempt; it was never acknowledged.
        shutil.rmtree(target)
    events = events_for_batch(settings, batch_id)
    normalized: list[dict] = []
    for event in events:
        item = dict(event)
        before = item.get("record_before")
        after = item.get("record_after")
        # Spark infers one schema per JSON field. Populate the absent image with
        # its counterpart so all CDC operation shapes remain compatible.
        item["record_before"] = before if before is not None else after
        item["record_after"] = after if after is not None else before
        normalized.append(item)
    temporary = target.with_name(target.name + ".partial")
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir(parents=True, exist_ok=False)
    (temporary / "events.jsonl").write_text("\n".join(json.dumps(event, default=str, sort_keys=True) for event in normalized) + "\n", encoding="utf-8")
    (temporary / "manifest.json").write_text(json.dumps({"format_version": 2, "batch_id": batch_id, "event_count": len(normalized), "created_at": datetime.now(timezone.utc).isoformat()}, sort_keys=True), encoding="utf-8")
    temporary.rename(target)
    set_batch_state(settings, batch_id, "LANDED", {"event_count": len(normalized), "landing_path": str(target)})
    return target
def table_location(root: str, table: str) -> str:
    return str(Path(root) / "bronze" / table)

def register_table(spark: SparkSession, root: str, table: str) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table} USING DELTA LOCATION '{table_location(root, table)}'")

def apply_batch(settings: Settings, batch_id: str) -> dict:
    source = land_events(settings, batch_id) / "events.jsonl"
    spark = spark_session(settings.lakehouse_root)
    try:
        events = spark.read.json(str(source))
        events.write.format("delta").mode("append").option("txnAppId", "retail_bronze_events").option("txnVersion", int(events.agg(F.max("event_id")).first()[0])).save(str(Path(settings.lakehouse_root) / "bronze_events"))
        counts: dict[str, int] = {}
        for table, key in PRIMARY_KEYS.items():
            selected = events.filter(F.col("table_name") == table).withColumn("payload", F.coalesce("record_after", "record_before"))
            if selected.rdd.isEmpty():
                continue
            # Encode the control columns with each payload before parsing. Joining only
            # on a business key would multiply records whenever a key changes twice.
            records = selected.select(
                F.to_json(
                    F.struct(
                        "event_id", "source_epoch", "operation", "occurred_at", "payload"
                    )
                ).alias("json")
            )
            staged = spark.read.json(records.rdd.map(lambda row: row.json)).select(
                "event_id", "source_epoch", "operation", "occurred_at", "payload.*"
            )
            staged = staged.withColumn("is_deleted", F.col("operation") == F.lit("D"))
            staged = staged.withColumn("_rank", F.row_number().over(Window.partitionBy(key).orderBy(F.col("event_id").desc()))).filter("_rank = 1").drop("_rank")
            location = table_location(settings.lakehouse_root, table)
            if DeltaTable.isDeltaTable(spark, location):
                target = DeltaTable.forPath(spark, location)
                target.alias("target").merge(staged.alias("source"), f"target.{key}=source.{key}") \
                    .whenMatchedUpdateAll(condition="source.event_id > target.event_id") \
                    .whenNotMatchedInsertAll().execute()
            else:
                staged.write.format("delta").mode("errorifexists").save(location)
            register_table(spark, settings.lakehouse_root, table)
            counts[table] = staged.count()
        set_batch_state(settings, batch_id, "BRONZE_APPLIED", {"tables":counts})
        validation = {table: spark.read.format("delta").load(table_location(settings.lakehouse_root, table)).count() for table in counts}
        set_batch_state(settings, batch_id, "BRONZE_VALIDATED", {"table_rows":validation})
        acknowledge_batch(settings, batch_id)
        return {"batch_id":batch_id, "tables":counts, "table_rows":validation}
    except Exception as error:
        set_batch_state(settings, batch_id, "FAILED", {"stage":"bronze", "error":str(error)[:1000]})
        raise
    finally:
        spark.stop()

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_id")
    args = parser.parse_args()
    settings = Settings.from_env()
    print(json.dumps(apply_batch(settings, args.batch_id), default=str))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
