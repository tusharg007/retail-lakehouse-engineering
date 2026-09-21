CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.schema_migrations (
  migration_id text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS control.dataset_loads (
  dataset_hash text PRIMARY KEY,
  source_epoch uuid NOT NULL,
  loaded_at timestamptz NOT NULL DEFAULT now(),
  table_counts jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS control.change_events (
  event_id bigserial PRIMARY KEY,
  source_epoch uuid NOT NULL,
  transaction_id bigint NOT NULL,
  table_name text NOT NULL,
  business_key text NOT NULL,
  operation char(1) NOT NULL CHECK (operation IN ('I','U','D')),
  record_before jsonb,
  record_after jsonb,
  occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  acknowledged_at timestamptz,
  CONSTRAINT change_events_payload CHECK ((operation = 'D' AND record_before IS NOT NULL) OR (operation <> 'D' AND record_after IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS change_events_pending_idx ON control.change_events (source_epoch, event_id) WHERE acknowledged_at IS NULL;

CREATE TABLE IF NOT EXISTS control.batches (
  batch_id uuid PRIMARY KEY,
  source_epoch uuid NOT NULL,
  input_mode text NOT NULL CHECK (input_mode IN ('cdc','files')),
  state text NOT NULL CHECK (state IN ('CLAIMED','LANDED','BRONZE_APPLIED','BRONZE_VALIDATED','TRANSFORMED','PUBLISHED','FAILED')),
  event_ids bigint[] NOT NULL DEFAULT '{}',
  input_manifest jsonb,
  reconciliation jsonb,
  failure_stage text,
  failure_detail text,
  attempts integer NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS batches_active_idx ON control.batches (created_at) WHERE state NOT IN ('PUBLISHED');

CREATE TABLE IF NOT EXISTS control.source_health_checks (
  check_id bigserial PRIMARY KEY,
  input_mode text NOT NULL,
  checked_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL CHECK (status IN ('SUCCESS','FAILED')),
  detail jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS control.published_releases (
  release_id uuid PRIMARY KEY,
  batch_id uuid NOT NULL REFERENCES control.batches(batch_id),
  state text NOT NULL CHECK (state IN ('CANDIDATE','PUBLISHED','FAILED')),
  checks jsonb NOT NULL,
  published_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS control.release_pointer (
  pointer_name text PRIMARY KEY CHECK (pointer_name = 'dashboard'),
  release_id uuid REFERENCES control.published_releases(release_id),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION control.capture_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE key_column text := TG_ARGV[0];
DECLARE source_id uuid;
BEGIN
  SELECT source_epoch INTO source_id FROM control.dataset_loads ORDER BY loaded_at DESC LIMIT 1;
  IF source_id IS NULL THEN
    source_id := '00000000-0000-0000-0000-000000000000'::uuid;
  END IF;
  INSERT INTO control.change_events(source_epoch, transaction_id, table_name, business_key, operation, record_before, record_after)
  VALUES (
    source_id,
    txid_current(),
    TG_TABLE_NAME,
    CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD)->>key_column ELSE to_jsonb(NEW)->>key_column END,
    substr(TG_OP, 1, 1),
    CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
    CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) END
  );
  RETURN COALESCE(NEW, OLD);
END;
$$;
