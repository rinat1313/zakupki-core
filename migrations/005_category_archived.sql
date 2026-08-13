-- Category archive flag (hidden in UI, kept in DB).
ALTER TABLE categories
  ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS categories_archived_idx ON categories (archived);
