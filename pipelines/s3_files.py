"""S3 manifest validation for the alternative complete-snapshot input mode."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import boto3
from botocore.config import Config
from pipelines.config import Settings

TABLES = ("customers","stores","products","employees","orders","order_items")

def client():
    return boto3.client("s3", config=Config(retries={"max_attempts": 5, "mode": "standard"}))

def check(settings: Settings) -> dict:
    if not settings.s3_bucket or not settings.s3_prefix:
        raise ValueError("S3_BUCKET and S3_PREFIX are required for S3 operations")
    paginator = client().get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=settings.s3_bucket, Prefix=settings.s3_prefix, PaginationConfig={"MaxItems": 10})
    objects = [entry["Key"] for page in pages for entry in page.get("Contents", [])]
    return {"bucket":settings.s3_bucket, "prefix":settings.s3_prefix, "status":"accessible", "sample_object_count":len(objects), "sample_keys":objects}

def download_manifest(settings: Settings, manifest_key: str, landing_dir: Path) -> dict:
    if not settings.s3_bucket or not manifest_key.startswith(settings.s3_prefix or ""):
        raise ValueError("Manifest key must be within S3_PREFIX")
    s3 = client()
    manifest = json.loads(s3.get_object(Bucket=settings.s3_bucket, Key=manifest_key)["Body"].read())
    required = {"dataset_id","source_epoch","snapshot_sequence","schema_version","objects"}
    if required - manifest.keys() or set(manifest["objects"]) != set(TABLES):
        raise ValueError("Manifest must describe all six source tables")
    destination = landing_dir / f"snapshot={manifest['snapshot_sequence']}"
    destination.mkdir(parents=True, exist_ok=False)
    for table in TABLES:
        descriptor = manifest["objects"][table]
        key = descriptor["key"]
        if not key.startswith(settings.s3_prefix or ""):
            raise ValueError(f"Object is outside S3_PREFIX: {key}")
        target = destination / f"{table}.csv"
        arguments = {"VersionId": descriptor["version_id"]} if descriptor.get("version_id") else {}
        s3.download_file(settings.s3_bucket, key, str(target), ExtraArgs=arguments)
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        if digest != descriptor["sha256"]:
            raise ValueError(f"Checksum mismatch for {table}")
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return {"snapshot_sequence":manifest["snapshot_sequence"], "landing_dir":str(destination)}

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("check",))
    args = parser.parse_args()
    if args.action == "check":
        print(json.dumps(check(Settings.from_env())))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
