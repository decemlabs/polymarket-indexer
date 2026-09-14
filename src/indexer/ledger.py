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
            WHERE wallet = ?
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
              AND (balance = '0' OR balance = '0.0' OR CAST(balance AS REAL) = 0);
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
            WHERE wallet = $1
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
            "DELETE FROM current_balances WHERE wallet = $1 AND balance = 0;",
            [target_wallet],
        )

    pos_count = await CurrentBalance.filter(
        wallet=target_wallet, balance__gt=0
    ).count()
    neg_count = await CurrentBalance.filter(
        wallet=target_wallet, balance__lt=0
    ).count()

    if neg_count > 0:
        logger.error(
            f"CRITICAL: Found {neg_count:,} NEGATIVE balance positions for {target_wallet}! "
            "Ledger invariant violated (local calculation contains missing or corrupted events)."
        )

    logger.info(
        f"Replay complete for {target_wallet}: {pos_count:,} active positions"
        + (f", {neg_count:,} NEGATIVE positions." if neg_count > 0 else ".")
    )
    return int(pos_count)


async def get_current_balances(
    wallet: str | None = None,
    include_zero: bool = False,
) -> list[dict[str, Any]]:
    target_wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
    positions = await CurrentBalance.filter(
        wallet=target_wallet,
    ).values("token_type", "token_address", "token_id", "balance", "updated_at")

    if not include_zero:
        positions = [
            p for p in positions if Decimal(str(p["balance"])) != Decimal(0)
        ]

    # Negative balances are critical anomalies and must be sorted to the very top,
    # followed by ERC-20, then ERC-1155 sorted by absolute balance descending.
    positions.sort(
        key=lambda x: (
            0 if Decimal(str(x["balance"])) < 0 else 1,
            0 if x["token_type"] == "ERC20" else 1,
            -abs(Decimal(str(x["balance"]))),
        ),
    )
    return positions
