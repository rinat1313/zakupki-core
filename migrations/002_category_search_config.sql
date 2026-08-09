-- Bind category (tender list) to an external search-system configuration id.
ALTER TABLE categories
  ADD COLUMN IF NOT EXISTS search_config_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS categories_search_config_id_uidx
  ON categories (search_config_id)
  WHERE search_config_id IS NOT NULL AND search_config_id <> '';
