-- Workspace retention: tenders taken into work / marked interesting survive search-pool sync.
ALTER TABLE tenders
  ADD COLUMN IF NOT EXISTS retained BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS retained_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS retain_reason TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS tenders_retained_idx ON tenders (retained) WHERE retained;
