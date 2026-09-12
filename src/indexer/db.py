import logging
from decimal import Decimal
from typing import Any

from tortoise import Tortoise
from tortoise.functions import Count, Max, Min

from . import models as models_module
from .config import settings
from .models import BalanceChange, Checkpoint, CurrentBalance, RawLog

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
    legacy_e_row = await BalanceChange.filter(token_id__icontains="e").first()
    if legacy_e_row:
        all_e_rows = await BalanceChange.filter(token_id__icontains="e").values(
            "id", "token_id"
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

    await CurrentBalance.filter(token_id__icontains="e").delete()


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
    agg = (
        await RawLog.all()
        .annotate(
            total=Count("id"),
            min_b=Min("block_number"),
            max_b=Max("block_number"),
        )
        .values("total", "min_b", "max_b")
    )

    events = (
        await RawLog.all()
        .annotate(cnt=Count("id"))
        .group_by("event_name")
        .order_by("-cnt")
        .values("event_name", "cnt")
    )
    contracts = (
        await RawLog.all()
        .annotate(cnt=Count("id"))
        .group_by("contract_address")
        .order_by("-cnt")
        .values("contract_address", "cnt")
    )

    stats: dict[str, Any] = {
        "total_logs": agg[0]["total"] if agg and agg[0] else 0,
        "min_block": agg[0]["min_b"] if agg and agg[0] else None,
        "max_block": agg[0]["max_b"] if agg and agg[0] else None,
        "by_event": {r["event_name"]: r["cnt"] for r in events},
        "by_contract": {r["contract_address"]: r["cnt"] for r in contracts},
    }
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
    total = await BalanceChange.all().count()
    ops = (
        await BalanceChange.all()
        .annotate(cnt=Count("id"))
        .group_by("operation_type")
        .order_by("-cnt")
        .values("operation_type", "cnt")
    )
    tokens = (
        await BalanceChange.all()
        .annotate(cnt=Count("id"))
        .group_by("token_type")
        .order_by("-cnt")
        .values("token_type", "cnt")
    )

    return {
        "total_changes": total,
        "by_operation": {r["operation_type"]: r["cnt"] for r in ops},
        "by_token_type": {r["token_type"]: r["cnt"] for r in tokens},
    }
