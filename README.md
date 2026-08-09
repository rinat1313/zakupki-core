# zakupki-core

Доменный сервис платформы закупок (Go + PostgreSQL):

- категории, тендеры, документы, заказчики
- очередь ingest (CSV → jobs → items)
- worker вызывает **zakupki-parser** (`PARSER_URL`) и пишет результат в PG
- AI-оценка через **analizator_zakupok** (`ANALIZATOR_URL`)

## Env

| Переменная | Пример |
|------------|--------|
| `DATABASE_URL` | `postgres://zakupki:zakupki@localhost:5432/zakupki?sslmode=disable` |
| `HTTP_ADDR` | `:8080` |
| `PARSER_URL` | `http://parser:8091` |
| `ANALIZATOR_URL` | `http://analizator:8088` |

## API

HTTP API `/api/v1/...` (категории, ingest, тендеры, заказчики, воркеры, AI).

**Swagger / OpenAPI**

| URL | Описание |
|-----|----------|
| `/swagger/` | Swagger UI |
| `/api/openapi.yaml` | OpenAPI 3.0 спецификация |

Файлы в репозитории: `docs/openapi.yaml`, `internal/httpapi/openapi.yaml`.

```bash
export DATABASE_URL=postgres://zakupki:zakupki@127.0.0.1:5432/zakupki?sslmode=disable
export PARSER_URL=http://127.0.0.1:8091
go run ./cmd/core
# открыть http://127.0.0.1:8080/swagger/
```

Миграции: `migrations/001_init.sql` (применяются из zakupki-platform / postgres init).

Полный стек: [zakupki-platform](https://github.com/rinat1313/zakupki-platform).
