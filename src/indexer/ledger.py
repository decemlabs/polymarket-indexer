import logging
from typing import Any

from tortoise import Tortoise
from web3 import Web3

from .config import settings
from .models import CurrentBalance

logger = logging.getLogger(__name__)


async def update_current_balances(wallet: str | None = None) -> int:
    """
    Replays all balance_changes and updates current_balances via atomic database upsert.
    Correctly cleans up zero positions and handles SQLite vs PostgreSQL type differences.
    Returns the count of active non-zero positions.
    """
    target_wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
    conn = Tortoise.get_connection("default")
    is_sqlite = conn.capabilities.dialect == "sqlite"

    if is_sqlite:
        # In SQLite, format SUM(amount_delta) without decimals using PRINTF('%.0f', ...)
        # Scope by wallet (or NULL for backwards compatibility)
        await conn.execute_query(
            """
            INSERT INTO current_balances (wallet, token_type, token_address, token_id, balance, updated_at)
            SELECT
                ?,
                token_type,
                token_address,
                token_id,
                PRINTF('%.0f', SUM(amount_delta)) as balance,
                CURRENT_TIMESTAMP
            FROM balance_changes
            WHERE wallet = ? OR wallet IS NULL
            GROUP BY token_type, token_address, token_id
            ON CONFLICT (wallet, token_address, token_id)
            DO UPDATE SET
                token_type = EXCLUDED.token_type,
                balance = EXCLUDED.balance,
                updated_at = CURRENT_TIMESTAMP;
            """,
            [target_wallet, target_wallet],
        )

        # Clean up zero or negative balances (handles string representations '0', '0.0')
        await conn.execute_query(
            """
            DELETE FROM current_balances
            WHERE wallet = ?
              AND (balance = '0' OR balance = '0.0' OR CAST(balance AS REAL) <= 0);
            """,
            [target_wallet],
        )

        # Count active positions avoiding SQLite text affinity trap ('0' > 0 is true in SQLite)
        res = await conn.execute_query_dict(
            "SELECT COUNT(*) as cnt FROM current_balances WHERE wallet = ? AND CAST(balance AS REAL) > 0;",
            [target_wallet],
        )
        active_count = res[0]["cnt"] if res else 0

    else:
        # PostgreSQL path
        await conn.execute_query(
            """
            INSERT INTO current_balances (wallet, token_type, token_address, token_id, balance, updated_at)
            SELECT
                $1,
                token_type,
                token_address,
                token_id,
                SUM(amount_delta)::numeric(78, 0) as balance,
                NOW()
            FROM balance_changes
            WHERE wallet = $1 OR wallet IS NULL
            GROUP BY token_type, token_address, token_id
            ON CONFLICT (wallet, token_address, token_id)
            DO UPDATE SET
                token_type = EXCLUDED.token_type,
                balance = EXCLUDED.balance,
                updated_at = NOW();
            """,
            [target_wallet],
        )

        await conn.execute_query(
            "DELETE FROM current_balances WHERE wallet = $1 AND balance <= 0;",
            [target_wallet],
        )

        active_count = await CurrentBalance.filter(
            wallet=target_wallet, balance__gt=0
        ).count()

    logger.info(
        f"Replay complete for {target_wallet}: {active_count:,} active non-zero positions."
    )
    return int(active_count)


async def get_current_balances(wallet: str | None = None) -> list[dict[str, Any]]:
    target_wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
    conn = Tortoise.get_connection("default")
    is_sqlite = conn.capabilities.dialect == "sqlite"

    if is_sqlite:
        # Raw query to properly sort numerically in SQLite (avoiding alphabetical string sorting)
        return await conn.execute_query_dict(
            """
            SELECT token_type, token_address, token_id, balance, updated_at
            FROM current_balances
            WHERE wallet = ? AND CAST(balance AS REAL) > 0
            ORDER BY token_type, CAST(balance AS REAL) DESC;
            """,
            [target_wallet],
        )

    return (
        await CurrentBalance.filter(
            wallet=target_wallet,
            balance__gt=0,
        )
        .order_by("token_type", "-balance")
        .values("token_type", "token_address", "token_id", "balance", "updated_at")
    )
