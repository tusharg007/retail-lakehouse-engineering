#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION="${1:?usage: project.sh {init|doctor|up|stop|status|logs|load|run|verify|s3-check|s3-upload}}"
shift || true
compose() { docker compose --project-directory "$ROOT" "$@"; }
case "$ACTION" in
  init)
    [[ -f "$ROOT/.env" ]] || cp "$ROOT/.env.example" "$ROOT/.env"
    echo "Create private local passwords in .env, then set your AWS S3 values before S3 actions."
    ;;
  doctor) compose config --quiet; docker version; ;;
  up) compose up -d --build source-postgres airflow-postgres spark-thrift airflow-init airflow-webserver airflow-scheduler ;;
  stop) compose stop ;;
  status) compose ps ;;
  logs) compose logs --tail 100 ;;
  load) compose run --rm airflow-scheduler python /opt/retail/walmart_dataset/load_data.py ;;
  run) compose exec airflow-scheduler airflow dags trigger retail_lakehouse ;;
  s3-check) compose run --rm airflow-scheduler python -m pipelines.s3_files check ;;
  s3-upload) compose run --rm airflow-scheduler python -m pipelines.s3_upload --sequence "${1:?sequence required}" ;;
  verify) "$ROOT/scripts/audit_dataset.ps1"; compose run --rm airflow-scheduler dbt parse --project-dir /opt/retail/airflow_dbt_project/walmart_project --profiles-dir /opt/retail/airflow_dbt_project/walmart_project ;;
  *) exit 2 ;;
esac
