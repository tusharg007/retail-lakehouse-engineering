"""Upload immutable full-snapshot CSV bundles to the configured S3 prefix."""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pipelines.config import Settings
from pipelines.s3_files import TABLES, client

def upload_snapshot(data_dir: Path, sequence: int, settings: Settings) -> dict:
    if not settings.s3_bucket or not settings.s3_prefix:
        raise ValueError("S3_BUCKET and S3_PREFIX are required")
    prefix = settings.s3_prefix.rstrip("/") + f"/snapshots/{sequence:010d}/"
    s3 = client()
    objects: dict[str, dict] = {}
    for table in TABLES:
        source = data_dir / f"{table}.csv"
        if not source.is_file():
            raise FileNotFoundError(source)
        content_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        key = prefix + source.name
        response = s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=source.read_bytes(), Metadata={"sha256":content_hash, "table":table})
        objects[table] = {"key":key, "sha256":content_hash, "row_count":sum(1 for _ in source.open(encoding="utf-8-sig")) - 1, "version_id":response.get("VersionId")}
    manifest = {"dataset_id":"owner-supplied-retail", "source_epoch":str(uuid.uuid4()), "snapshot_sequence":sequence, "schema_version":1, "objects":objects, "created_at":datetime.now(timezone.utc).isoformat()}
    manifest_key = prefix + "manifest.json"
    response = s3.put_object(Bucket=settings.s3_bucket, Key=manifest_key, Body=json.dumps(manifest, sort_keys=True).encode(), ContentType="application/json")
    return {"manifest_key":manifest_key, "manifest_version_id":response.get("VersionId"), "snapshot_sequence":sequence}

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence", type=int, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[1] / "walmart_dataset" / "data")
    args = parser.parse_args()
    print(json.dumps(upload_snapshot(args.data_dir, args.sequence, Settings.from_env())))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
