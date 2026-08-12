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
| Sync пула (контракт search) | `POST /api/v1/categories/by-search-config/{id}/sync` `{items, enqueue}` → upsert tenders + optional ingest |
| Auto-AI для поисковика | `PUT /api/v1/categories/by-search-profile/{id}/auto-ai` `{enabled:true}` |
| Сохранить тендер вне пула | `POST /api/v1/tenders/{id}/retain` (`interesting` / `in_work` / `manual`) |
| Workspace (сохранённые) | `GET /api/v1/tenders?retained=true` |
| Тендеры поисковика | `GET /api/v1/tenders?search_profile_id=...` (алиас `search_config_id`) |

### Retention (чтобы не потерять закупки при смене поиска)

Пул поисковика может обновляться (sync). Тендеры с `retained=true` **не удаляются** из БД:
их только отвязывают от списка поиска. Они остаются в workspace.

Авто-retain:
- старт AI-анализа (`analyzing`)
- AI-рекомендация `participate` / `caution`

Ручной retain: интересная / взяли в работу.

**Сбор + AI по выбранному поисковику (UI «Поисковики»):**
1. Search/gateway шлёт sync snapshot → core создаёт/обновляет список (`search_profile_id`)
2. Ingest воркеры собирают карточки/документы (`enqueue: true`)
3. `PUT .../auto-ai {enabled:true}` на категории поисковика → auto-AI берёт только его готовые тендеры
4. UI читает `GET /tenders?search_profile_id=...` (`collect_pct`, `ai_pct`, `assess_summary`, `card_tone`)

```json
POST /api/v1/search-profiles/{search_profile_id}/sync
{
  "search_profile_id": "…",
  "config_version": 3,
  "items": [{"reg_number":"…","notice_url":"https://zakupki.gov.ru/…"}],
  "enqueue": true
}
```

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
