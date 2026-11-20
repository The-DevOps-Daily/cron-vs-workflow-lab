-- One schema per setup, so all setups can run side by side against one warehouse.
-- naive and k8s_append keep the table most first versions have: a surrogate key
-- and no unique constraint on the business key. The others add one.
-- Runs as the etl role, so it owns the schemas and tables.
SET ROLE etl;

DO $$
DECLARE s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['naive', 'k8s_append'] LOOP
    EXECUTE format('CREATE SCHEMA %I', s);
    EXECUTE format('CREATE TABLE %I.orders (id bigserial PRIMARY KEY, order_id text NOT NULL,
      partition text NOT NULL, customer text NOT NULL, amount_cents int NOT NULL,
      loaded_at timestamptz NOT NULL DEFAULT now())', s);
    EXECUTE format('CREATE TABLE %I.reports_sent (partition text PRIMARY KEY, run_id text,
      sent_at timestamptz NOT NULL DEFAULT now())', s);
  END LOOP;
  FOREACH s IN ARRAY ARRAY['hardened', 'k8s', 'kestra', 'prefect'] LOOP
    EXECUTE format('CREATE SCHEMA %I', s);
    EXECUTE format('CREATE TABLE %I.orders (order_id text PRIMARY KEY,
      partition text NOT NULL, customer text NOT NULL, amount_cents int NOT NULL,
      loaded_at timestamptz NOT NULL DEFAULT now())', s);
    EXECUTE format('CREATE TABLE %I.reports_sent (partition text PRIMARY KEY, run_id text,
      sent_at timestamptz NOT NULL DEFAULT now())', s);
  END LOOP;
END $$;
