-- platform PostgreSQL schema
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE analysis_status AS ENUM (
  'none', 'analyzed', 'delete', 'irrelevant', 'past', 'other'
);

CREATE TYPE doc_process_status AS ENUM (
  'processed', 'unprocessed'
);

CREATE TYPE ingest_item_status AS ENUM (
  'queued', 'running', 'ok', 'error', 'skipped', 'unsupported_source', 'failed_analyze'
);

CREATE TABLE categories (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug        TEXT NOT NULL UNIQUE,
  title       TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE customers (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  inn           TEXT NOT NULL DEFAULT '',
  kpp           TEXT NOT NULL DEFAULT '',
  ogrn          TEXT NOT NULL DEFAULT '',
  full_name     TEXT NOT NULL DEFAULT '',
  short_name    TEXT NOT NULL DEFAULT '',
  address       TEXT NOT NULL DEFAULT '',
  email         TEXT NOT NULL DEFAULT '',
  phone         TEXT NOT NULL DEFAULT '',
  contact_person TEXT NOT NULL DEFAULT '',
  organization_code TEXT NOT NULL DEFAULT '',
  agency_id     TEXT NOT NULL DEFAULT '',
  payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (inn, kpp)
);

CREATE TABLE tenders (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reg_number       TEXT NOT NULL,
  source_site      TEXT NOT NULL,
  law              TEXT NOT NULL DEFAULT '',
  customer_id      UUID REFERENCES customers(id) ON DELETE SET NULL,
  object_name      TEXT NOT NULL DEFAULT '',
  status           TEXT NOT NULL DEFAULT '',
  nmck             NUMERIC(20,2),
  currency         TEXT NOT NULL DEFAULT 'RUB',
  published_at     TIMESTAMPTZ,
  updated_on_site  TIMESTAMPTZ,
  application_end  TIMESTAMPTZ,
  analysis_status  analysis_status NOT NULL DEFAULT 'none',
  payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (reg_number, source_site)
);

CREATE INDEX tenders_analysis_status_idx ON tenders(analysis_status);
CREATE INDEX tenders_application_end_idx ON tenders(application_end);
CREATE INDEX tenders_reg_number_idx ON tenders(reg_number);

CREATE TABLE tender_categories (
  tender_id    UUID NOT NULL REFERENCES tenders(id) ON DELETE CASCADE,
  category_id  UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  PRIMARY KEY (tender_id, category_id)
);

CREATE TABLE documents (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tender_id       UUID NOT NULL REFERENCES tenders(id) ON DELETE CASCADE,
  uid             TEXT NOT NULL DEFAULT '',
  filename        TEXT NOT NULL DEFAULT '',
  source_url      TEXT NOT NULL,
  group_title     TEXT NOT NULL DEFAULT '',
  edition         TEXT NOT NULL DEFAULT '',
  process_status  doc_process_status NOT NULL DEFAULT 'unprocessed',
  text_content    TEXT,
  process_error   TEXT NOT NULL DEFAULT '',
  content_hash    TEXT NOT NULL DEFAULT '',
  removed         BOOLEAN NOT NULL DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tender_id, source_url)
);

CREATE INDEX documents_tender_idx ON documents(tender_id);
CREATE INDEX documents_process_status_idx ON documents(process_status);

CREATE TABLE tender_events (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tender_id   UUID NOT NULL REFERENCES tenders(id) ON DELETE CASCADE,
  event_type  TEXT NOT NULL,
  message     TEXT NOT NULL DEFAULT '',
  details     JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX tender_events_tender_idx ON tender_events(tender_id, created_at DESC);

CREATE TABLE tender_assessments (
  tender_id   UUID PRIMARY KEY REFERENCES tenders(id) ON DELETE CASCADE,
  summary     TEXT NOT NULL DEFAULT '',
  score       NUMERIC(8,2),
  details     JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- stubs for future integrations
CREATE TABLE customer_courts (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id  UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  case_number  TEXT NOT NULL DEFAULT '',
  court_name   TEXT NOT NULL DEFAULT '',
  status       TEXT NOT NULL DEFAULT '',
  details      JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE customer_rnp (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id  UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  registry_number TEXT NOT NULL DEFAULT '',
  reason       TEXT NOT NULL DEFAULT '',
  included_at  TIMESTAMPTZ,
  details      JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ingest_jobs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  category_id   UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  source_name   TEXT NOT NULL DEFAULT '',
  status        TEXT NOT NULL DEFAULT 'queued',
  total_items   INT NOT NULL DEFAULT 0,
  done_items    INT NOT NULL DEFAULT 0,
  error_items   INT NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ingest_job_items (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id        UUID NOT NULL REFERENCES ingest_jobs(id) ON DELETE CASCADE,
  reg_number    TEXT NOT NULL,
  source_site   TEXT NOT NULL,
  status        ingest_item_status NOT NULL DEFAULT 'queued',
  error         TEXT NOT NULL DEFAULT '',
  tender_id     UUID REFERENCES tenders(id) ON DELETE SET NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ingest_job_items_status_idx ON ingest_job_items(status);
CREATE INDEX ingest_job_items_job_idx ON ingest_job_items(job_id);

CREATE TABLE ingest_job_logs (
  id           BIGSERIAL PRIMARY KEY,
  job_id       UUID NOT NULL REFERENCES ingest_jobs(id) ON DELETE CASCADE,
  item_id      UUID REFERENCES ingest_job_items(id) ON DELETE SET NULL,
  reg_number   TEXT NOT NULL DEFAULT '',
  level        TEXT NOT NULL DEFAULT 'info',
  message      TEXT NOT NULL DEFAULT '',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ingest_job_logs_job_idx ON ingest_job_logs(job_id, id);
