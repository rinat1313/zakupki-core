-- Per-searcher Auto-AI and sync versioning (zakupki-search / gateway UI).
ALTER TABLE categories
  ADD COLUMN IF NOT EXISTS auto_ai BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS synced_config_version BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS categories_auto_ai_idx ON categories (auto_ai) WHERE auto_ai;
