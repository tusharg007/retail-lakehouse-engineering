from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    source_dsn: str
    lakehouse_root: str
    input_mode: str
    s3_bucket: str | None
    s3_prefix: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        mode = os.getenv("INPUT_MODE", "cdc")
        if mode not in {"cdc", "files"}:
            raise ValueError("INPUT_MODE must be cdc or files")
        dsn = os.getenv("RETAIL_SOURCE_DSN")
        if not dsn:
            raise ValueError("RETAIL_SOURCE_DSN is required")
        return cls(dsn, os.getenv("RETAIL_LAKEHOUSE_ROOT", "/opt/lakehouse"), mode, os.getenv("S3_BUCKET") or None, os.getenv("S3_PREFIX") or None)
