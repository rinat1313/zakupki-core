# zakupki-core

Доменный сервис платформы закупок (Go + PostgreSQL):

- **списки тендеров** (`categories`) с привязкой к конфигурации поисковика (`search_config_id`)
- тендеры, документы, заказчики
- очередь ingest (CSV / JSON items → jobs → items)
- worker вызывает **zakupki-parser** (`PARSER_URL`) и пишет результат в PG
- AI-оценка через **analizator_zakupok** (`ANALIZATOR_URL`)

## Списки и поисковик

`category` = список тендеров. Поле `search_config_id` — уникальный id настройки поисковой системы
(внешний сервис конфигурации поиска). Все тендеры списка привязаны к категории через
`tender_categories`, а категория — к `search_config_id`.

| Операция | Метод |
|----------|--------|
| Создать список (+ `search_config_id`) | `POST /api/v1/categories` |
| Список / получить / по search config | `GET /api/v1/categories`, `.../{slug}`, `.../by-search-config/{id}` |
| Редактировать | `PATCH /api/v1/categories/{slug}` |
| Удалить список | `DELETE /api/v1/categories/{slug}` |
| Тендеры списка | `GET /api/v1/tenders?search_config_id=...` или `?category=slug` |
| Загрузить CSV в список | `POST /api/v1/ingest` (`search_config_id` / `category_slug`) |
| Пуш результатов поисковика | `POST /api/v1/ingest/items` (JSON + `search_config_id`) |

## Env

| Переменная | Пример |
|------------|--------|
| `DATABASE_URL` | `postgres://zakupki:zakupki@localhost:5432/zakupki?sslmode=disable` |
| `HTTP_ADDR` | `:8080` |
| `PARSER_URL` | `http://parser:8091` |
| `ANALIZATOR_URL` | `http://analizator:8088` |

## API

Совместим с прежним platform API (`/api/v1/...`). См. `zakupki-platform/contracts/openapi/`.

```bash
export DATABASE_URL=postgres://zakupki:zakupki@127.0.0.1:5432/zakupki?sslmode=disable
export PARSER_URL=http://127.0.0.1:8091
go run ./cmd/core
```

Миграции: `migrations/001_init.sql`, `migrations/002_category_search_config.sql`
(также применяются идемпотентно из `store.Migrate` при старте).

Полный стек: [zakupki-platform](https://github.com/rinat1313/zakupki-platform).
