import logging
from decimal import Decimal
from typing import Any

from tortoise import Tortoise

from . import models as models_module
from .config import settings
from .models import BalanceChange, Checkpoint, RawLog

logger = logging.getLogger(__name__)


def get_tortoise_db_url(raw_url: str | None = None) -> str:
    url = raw_url or settings.database_url
    if url.startswith("postgresql://"):
        return "postgres://" + url.removeprefix("postgresql://")
    return url


async def _migrate_sqlite_schema(conn: Any) -> None:
    cols = await conn.execute_query_dict("PRAGMA table_info(balance_changes);")
    col_names = {c["name"] for c in cols}
    if col_names and "wallet" not in col_names:
        logger.info("Migrating schema: adding 'wallet' column to balance_changes...")
        await conn.execute_script(
            "ALTER TABLE balance_changes ADD COLUMN wallet VARCHAR(42);"
        )

    # Миграция token_id из старой научной нотации в обычные строки
    legacy_e_rows = await conn.execute_query_dict(
        "SELECT id, token_id FROM balance_changes WHERE token_id LIKE '%E%' OR token_id LIKE '%e%' LIMIT 1;"
    )
    if legacy_e_rows:
        all_e_rows = await conn.execute_query_dict(
            "SELECT id, token_id FROM balance_changes WHERE token_id LIKE '%E%' OR token_id LIKE '%e%';"
        )
        logger.info(
            f"Found {len(all_e_rows):,} legacy scientific token_ids. Canonicalizing to exact decimal strings..."
        )
        updates = [[str(int(Decimal(r["token_id"]))), r["id"]] for r in all_e_rows]
        await conn.execute_many(
            "UPDATE balance_changes SET token_id = ? WHERE id = ?;", updates
        )
        logger.info(
            f"Successfully canonicalized {len(updates):,} token_ids in balance_changes."
        )

    await conn.execute_query(
        "DELETE FROM current_balances WHERE token_id LIKE '%E%' OR token_id LIKE '%e%';"
    )


async def init_db() -> None:
    db_url = get_tortoise_db_url()
    logger.info("Initializing Tortoise ORM...")
    await Tortoise.init(
        db_url=db_url,
        modules={"models": [models_module.__name__]},
    )
    await Tortoise.generate_schemas()
    conn = Tortoise.get_connection("default")
    if conn.capabilities.dialect == "sqlite":
        await conn.execute_script("""
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -64000;
            PRAGMA busy_timeout = 30000;
        """)
        await _migrate_sqlite_schema(conn)
    logger.info("Tortoise ORM schemas generated and ready.")


async def close_db() -> None:
    await Tortoise.close_connections()


async def get_checkpoint(checkpoint_id: str, default: int) -> int:
    cp = await Checkpoint.filter(id=checkpoint_id).first()
    if cp:
        return int(cp.last_scanned_block)
    return default


async def save_checkpoint(checkpoint_id: str, block_number: int) -> None:
    await Checkpoint.update_or_create(
        id=checkpoint_id,
        defaults={"last_scanned_block": block_number},
    )


async def insert_raw_logs(logs: list[dict[str, Any]]) -> int:
    if not logs:
        return 0

    model_instances = [RawLog(**l) for l in logs]
    await RawLog.bulk_create(
        model_instances,
        ignore_conflicts=True,
        batch_size=1000,
    )
    return len(logs)


async def get_raw_logs_stats() -> dict[str, Any]:
    stats: dict[str, Any] = {
        "total_logs": 0,
        "min_block": None,
        "max_block": None,
        "by_event": {},
        "by_contract": {},
    }
    conn = Tortoise.get_connection("default")

    agg = await conn.execute_query_dict(
        "SELECT COUNT(*) as total, MIN(block_number) as min_b, MAX(block_number) as max_b FROM raw_logs"
    )
    if agg and agg[0]:
        stats["total_logs"] = agg[0]["total"] or 0
        stats["min_block"] = agg[0]["min_b"]
        stats["max_block"] = agg[0]["max_b"]

    events = await conn.execute_query_dict(
        "SELECT event_name, COUNT(*) as cnt FROM raw_logs GROUP BY event_name ORDER BY cnt DESC"
    )
    for r in events:
        stats["by_event"][r["event_name"]] = r["cnt"]

    contracts = await conn.execute_query_dict(
        "SELECT contract_address, COUNT(*) as cnt FROM raw_logs GROUP BY contract_address ORDER BY cnt DESC"
    )
    for r in contracts:
        stats["by_contract"][r["contract_address"]] = r["cnt"]

    return stats


async def insert_balance_changes(changes: list[dict[str, Any]]) -> int:
    if not changes:
        return 0

    model_instances = [BalanceChange(**c) for c in changes]
    await BalanceChange.bulk_create(
        model_instances,
        ignore_conflicts=True,
        batch_size=1000,
    )
    return len(changes)


async def get_balance_changes_stats() -> dict[str, Any]:
    stats: dict[str, Any] = {
        "total_changes": 0,
        "by_operation": {},
        "by_token_type": {},
    }
    conn = Tortoise.get_connection("default")

    total_res = await conn.execute_query_dict(
        "SELECT COUNT(*) as total FROM balance_changes"
    )
    if total_res and total_res[0]:
        stats["total_changes"] = total_res[0]["total"] or 0

    ops = await conn.execute_query_dict(
        "SELECT operation_type, COUNT(*) as cnt FROM balance_changes GROUP BY operation_type ORDER BY cnt DESC"
    )
    for r in ops:
        stats["by_operation"][r["operation_type"]] = r["cnt"]

    tokens = await conn.execute_query_dict(
        "SELECT token_type, COUNT(*) as cnt FROM balance_changes GROUP BY token_type ORDER BY cnt DESC"
    )
    for r in tokens:
        stats["by_token_type"][r["token_type"]] = r["cnt"]

    return stats
