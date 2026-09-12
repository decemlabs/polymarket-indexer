# Polymarket On-Chain Indexer

Высокопроизводительный индексатор для восстановления полной истории и балансов кошелька в **Polymarket** напрямую через Polygon RPC и Tortoise ORM (SQLite / PostgreSQL).

## Принципы работы и архитектура

- **Источник истины**: только блокчейн Polygon через JSON-RPC (`eth_getLogs`, `balanceOf`).
- **Без сторонних API и контейнеров**: по умолчанию база данных — локальный **SQLite** (`polymarket.db`) в режиме WAL. Никаких Docker-контейнеров не требуется.
- **Топик-фильтрация**: запросы `eth_getLogs` выполняются с параллельной фильтрацией по 9 специализированным наборам топиков, что исключает перегрузку нод и лимиты.
- **Failover & Multi-RPC**: поддержка пула RPC-эндпоинтов, автоматическое переключение при 429/5xx ошибках, экспоненциальный backoff и авто-инжект Polygon POA middleware (`ExtraDataToPOAMiddleware`).
- **Адаптивный размер чанка**: от 20 000 до 200 000 блоков с динамическим уполовиниванием при лимитах RPC (`RangeLimitError`).
- **CQRS / Event Sourcing**:
  - `raw_logs`: неизменяемый лог он-чейн событий с защитой от дублей (`UNIQUE (transaction_hash, log_index)`);
  - `balance_changes`: нормализованный финансовый журнал проводок с определением операции (`TRADE_BUY`, `TRADE_SELL`, `SPLIT`, `MERGE`, `REDEEM`, `WRAP`, `UNWRAP`, `TRANSFER_IN`, `TRANSFER_OUT`);
  - `current_balances`: агрегированное состояние балансов;
  - `OnChainVerifier`: независимая сверка балансов с блокчейном через `balanceOf` до точного wei (0 diff).

## Быстрый старт

### 1. Требования
- Python >= 3.14
- [uv](https://github.com/astral-sh/uv)
- Контейнеры не требуются!

### 2. Конфигурация (.env)
Создайте `.env` при необходимости переопределить RPC или кошелек:
```bash
cp .env.example .env
```

Параметры:
```env
DATABASE_URL=sqlite://polymarket.db
WALLET=0x46b353667fd7d846af3bbeda6584b0e5b883d3de
START_BLOCK=80813420
CHUNK_SIZE=50000
POLYGON_RPC_URLS=https://polygon.gateway.tenderly.co,https://gateway.tenderly.co/public/polygon,https://rpc.private.mev-x.com/polygon,https://polygon-bor-rpc.publicnode.com
```

### 4. Доступные команды CLI

```bash
# 1. Показать текущий статус индексатора и он-чейн балансы
uv run polymarket-indexer status

# 2. Запустить сканирование он-чейн событий (с авто-нормализацией)
uv run polymarket-indexer scan

# Сканирование с ограничением чанков (например, 10 чанков по 5000 блоков):
uv run polymarket-indexer scan --chunks 10

# Сканирование конкретного диапазона блоков:
uv run polymarket-indexer scan --from-block 80813420 --to-block 80860000

# 3. Нормализация сырых логов в журнал balance_changes и пересчет балансов:
uv run polymarket-indexer normalize

# 4. Независимая он-чейн сверка рассчитанных балансов против RPC balanceOf:
uv run polymarket-indexer verify

# Проверка всех активных позиций:
uv run polymarket-indexer verify --all

# 5. Режим непрерывной live-синхронизации новых блоков сети:
uv run polymarket-indexer live --interval 3.0
```
