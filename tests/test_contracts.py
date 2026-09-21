from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def test_no_databricks_placeholder_in_active_profile():
    profile = (ROOT / "airflow_dbt_project" / "walmart_project" / "profiles.yml").read_text()
    assert "databricks" not in profile.lower()
    assert "type: spark" in profile

def test_s3_template_contains_no_secret_values():
    values = dict(line.split("=", 1) for line in (ROOT / ".env.example").read_text().splitlines() if "=" in line and not line.startswith("#"))
    assert values["AWS_ACCESS_KEY_ID"] == ""
    assert values["AWS_SECRET_ACCESS_KEY"] == ""

def test_model_uses_order_item_grain_without_employee_join():
    model = (ROOT / "airflow_dbt_project" / "walmart_project" / "models" / "v2" / "intermediate" / "order_items_enriched.sql").read_text().lower()
    assert "employees" not in model
    assert "order_item_id" in model
