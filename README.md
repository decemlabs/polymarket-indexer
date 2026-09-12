# Polymarket Indexer

Он-чейн индексатор сделок и балансов кошелька в Polymarket напрямую через Polygon RPC и Tortoise ORM (SQLite / PostgreSQL).

## Установка и запуск

Требуется Python >= 3.14 и менеджер пакетов [uv](https://github.com/astral-sh/uv). Дополнительные сервисы и Docker не нужны.

```bash
# 1. Установка зависимостей
uv sync

# 2. Настройка кошелька по умолчанию (опционально)
cp .env.example .env
```

Параметры в `.env`:
- `WALLET` — адрес целевого кошелька по умолчанию
- `DATABASE_URL` — строка подключения к БД (по умолчанию `sqlite://polymarket.db`)
- `POLYGON_RPC_URLS` — список RPC-нод через запятую

> **Входные данные — только адрес кошелька.**
> Начальный блок активности кошелька определяется автоматически за ~2 секунды через RPC, а размер пачки блоков (`chunk size`) динамически адаптируется под лимиты нод.

---

## Команды CLI

Адрес кошелька можно передавать напрямую в любую команду: `uv run polymarket-indexer <команда> [АДРЕС]` (если не передан — берется из `.env`):

| Команда | Описание | Пример |
|---|---|---|
| `status [АДРЕС]` | Статистика логов, рассчитанные и он-чейн балансы | `uv run polymarket-indexer status 0x46B3...` |
| `scan [АДРЕС]` | Сканирование он-чейн событий с авто-нормализацией | `uv run polymarket-indexer scan 0x46B3...` |
| `normalize [АДРЕС]` | Обработка логов в проводки и пересчет балансов | `uv run polymarket-indexer normalize` |
| `verify [АДРЕС]` | Сверка рассчитанных балансов с нодой (`balanceOf`) | `uv run polymarket-indexer verify --all` |
| `live [АДРЕС]` | Фоновая синхронизация новых блоков в реальном времени | `uv run polymarket-indexer live --interval 3.0` |

### Примеры:
```bash
# Сканирование произвольного кошелька (начальный блок определится автоматически):
uv run polymarket-indexer scan 0x46B353667FD7d846AF3BbEdA6584B0E5B883d3De

# Просмотр статуса и балансов кошелька из .env:
uv run polymarket-indexer status
```
