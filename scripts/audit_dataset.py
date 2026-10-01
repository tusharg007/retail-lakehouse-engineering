"""Audit the supplied CSV dataset without changing source data."""

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KEYS = {
    "customers": "customer_id",
    "employees": "employee_id",
    "order_items": "order_item_id",
    "orders": "order_id",
    "products": "product_id",
    "stores": "store_id",
}
RELATIONS = (
    ("employees", "store_id", "stores"),
    ("orders", "customer_id", "customers"),
    ("orders", "store_id", "stores"),
    ("order_items", "order_id", "orders"),
    ("order_items", "product_id", "products"),
)


def audit(data_directory: Path) -> dict:
    tables = {}
    indexes = {}
    summaries = []
    for name, key in KEYS.items():
        path = data_directory / f"{name}.csv"
        with path.open(newline="", encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            columns = reader.fieldnames or []
            if key not in columns:
                raise ValueError(f"Missing key: {name}.{key}")
            rows = list(reader)
        if not rows:
            raise ValueError(f"Empty input: {name}")

        index = {}
        duplicate_rows = blank_keys = 0
        for row in rows:
            identifier = row[key]
            if not identifier or not identifier.strip():
                blank_keys += 1
                continue
            if identifier in index:
                duplicate_rows += 1
            index[identifier] = row

        tables[name] = rows
        indexes[name] = index
        timestamps = [row["updated_timestamp"] for row in rows]
        summaries.append(
            {
                "table": name,
                "rows": len(rows),
                "primary_key": key,
                "duplicate_key_rows": duplicate_rows,
                "blank_keys": blank_keys,
                "columns": columns,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "minimum_updated_timestamp": min(timestamps),
                "maximum_updated_timestamp": max(timestamps),
                "is_active_values": sorted({row["is_active"] for row in rows}),
            }
        )

    relationships = []
    for child, column, parent in RELATIONS:
        missing = sum(
            not row[column] or not row[column].strip() or row[column] not in indexes[parent]
            for row in tables[child]
        )
        relationships.append(
            {"child": child, "column": column, "parent": parent, "missing_parent_rows": missing}
        )

    staff_counts = Counter(row["store_id"] for row in tables["employees"])
    line_sums = defaultdict(Decimal)
    item_counts = Counter()
    line_total = order_total = fanout_total = Decimal(0)
    fanout_rows = line_mismatches = order_mismatches = 0
    for item in tables["order_items"]:
        amount = Decimal(item["line_amount"])
        line_total += amount
        line_sums[item["order_id"]] += amount
        item_counts[item["order_id"]] += 1
        if Decimal(item["quantity"]) * Decimal(item["unit_price"]) != amount:
            line_mismatches += 1
        order = indexes["orders"].get(item["order_id"])
        staff = staff_counts.get(order["store_id"], 1) if order else 1
        fanout_rows += staff
        fanout_total += amount * staff

    for order in tables["orders"]:
        amount = Decimal(order["total_amount"])
        order_total += amount
        if amount != line_sums[order["order_id"]]:
            order_mismatches += 1

    return {
        "audit_scope": "Supplied CSV files; static analysis, not a pipeline execution.",
        "tables": summaries,
        "relationships": relationships,
        "reconciliation": {
            "order_line_rows": len(tables["order_items"]),
            "line_amount_sum": float(line_total),
            "order_total_amount_sum": float(order_total),
            "line_arithmetic_mismatches": line_mismatches,
            "order_total_mismatches": order_mismatches,
            "orders_with_multiple_lines": sum(count > 1 for count in item_counts.values()),
            "employee_store_join_rows": fanout_rows,
            "employee_store_join_line_amount_sum": float(fanout_total),
            "currency": "Unspecified in source; sums include all statuses and active flags.",
        },
        "observed_values": {
            "order_status": sorted({row["order_status"] for row in tables["orders"]}),
            "payment_method": sorted({row["payment_method"] for row in tables["orders"]}),
            "customer_email_domains": sorted({row["email"].split("@")[-1] for row in tables["customers"]}),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "retail_dataset" / "data")
    parser.add_argument("--output", type=Path, default=ROOT / "docs" / "evidence" / "dataset-audit.json")
    args = parser.parse_args()
    report = audit(args.data_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Dataset audit written to {args.output}")


if __name__ == "__main__":
    main()
