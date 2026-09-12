# Polymarket Indexer

Он-чейн индексатор сделок и балансов кошелька в Polymarket напрямую через Polygon RPC и Tortoise ORM (SQLite / PostgreSQL).

## Установка и запуск

Требуется Python >= 3.14 и менеджер пакетов [uv](https://github.com/astral-sh/uv). Дополнительные сервисы и Docker не нужны.

```bash
# 1. Установка зависимостей
uv sync

# 2. Настройка (опционально)
cp .env.example .env
```

Параметры в `.env`:
- `WALLET` — адрес целевого кошелька
- `DATABASE_URL` — строка подключения к БД (по умолчанию `sqlite://polymarket.db`)
- `START_BLOCK` — начальный блок сканирования (по умолчанию `80813420`)
- `CHUNK_SIZE` — размер диапазона блоков на запрос (по умолчанию `50000`)
- `POLYGON_RPC_URLS` — список RPC-нод через запятую

---

## Команды CLI

Запуск через `uv run polymarket-indexer <команда>` (или `uv run python -m src.indexer.main <команда>`):

| Команда | Описание | Пример |
|---|---|---|
| `status` | Текущий прогресс, статистика логов и он-чейн балансы | `uv run polymarket-indexer status` |
| `scan` | Сканирование событий из сети Polygon | `uv run polymarket-indexer scan --chunks 10` |
| `normalize` | Обработка логов в проводки и пересчет балансов | `uv run polymarket-indexer normalize` |
| `verify` | Сверка рассчитанных балансов с нодой (`balanceOf`) | `uv run polymarket-indexer verify --all` |
| `live` | Фоновая синхронизация новых блоков в реальном времени | `uv run polymarket-indexer live --interval 3.0` |

### Примеры сканирования:
```bash
# Сканирование с авто-нормализацией до актуального блока сети
uv run polymarket-indexer scan

# Сканирование конкретного диапазона блоков
uv run polymarket-indexer scan --from-block 80813420 --to-block 80900000
```
