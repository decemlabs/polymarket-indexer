import logging
from decimal import Decimal
from typing import Any

from tortoise import Tortoise
from web3 import Web3

from .config import settings
from .models import CurrentBalance

logger = logging.getLogger(__name__)


async def update_current_balances(wallet: str | None = None) -> int:
    target_wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
    conn = Tortoise.get_connection("default")
    is_sqlite = conn.capabilities.dialect == "sqlite"

    if is_sqlite:
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

        await conn.execute_query(
            """
            DELETE FROM current_balances
            WHERE wallet = ?
              AND (balance = '0' OR balance = '0.0' OR CAST(balance AS REAL) <= 0);
            """,
            [target_wallet],
        )

    else:
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
    positions = await CurrentBalance.filter(
        wallet=target_wallet,
        balance__gt=0,
    ).values("token_type", "token_address", "token_id", "balance", "updated_at")
    positions.sort(
        key=lambda x: (x["token_type"], -Decimal(str(x["balance"]))),
    )
    return positions
