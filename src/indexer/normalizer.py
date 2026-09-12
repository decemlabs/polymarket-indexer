import logging
from collections import defaultdict
from typing import Any

from eth_abi.abi import decode
from eth_abi.exceptions import DecodingError
from web3 import Web3

from .config import settings
from .contracts import (
    COLLATERAL_OFFRAMP,
    COLLATERAL_ONRAMP,
    COLLATERAL_TOKENS,
    CTF,
    EXCHANGE_CONTRACTS,
)
from .db import insert_balance_changes
from .models import RawLog

logger = logging.getLogger(__name__)


def normalize_transaction(
    tx_hash: str,
    logs: list[dict[str, Any]],
    wallet: str,
) -> list[dict[str, Any]]:
    wallet_lower = wallet.lower()
    balance_changes: list[dict[str, Any]] = []

    has_exchange = any(
        l["contract_address"] in EXCHANGE_CONTRACTS
        or "Order" in (l["event_name"] or "")
        for l in logs
    )
    has_split = any(l["event_name"] == "PositionSplit" for l in logs)
    has_merge = any(l["event_name"] == "PositionsMerge" for l in logs)
    has_redeem = any(l["event_name"] == "PayoutRedemption" for l in logs)
    has_onramp = any(l["contract_address"] == COLLATERAL_ONRAMP for l in logs)
    has_offramp = any(l["contract_address"] == COLLATERAL_OFFRAMP for l in logs)

    order_details: dict[str, Any] = {}
    for l in logs:
        if l.get("event_name") in (
            "OrderFilledV1",
            "OrderFilledV2",
            "OrdersMatchedV1",
            "OrdersMatchedV2",
        ):
            order_details["exchange_event"] = l["event_name"]
            order_details["exchange_contract"] = l["contract_address"]
            if l.get("topic1"):
                order_details["order_hash"] = l["topic1"]
            break

    wallet_sold_shares = False
    for l in logs:
        ev = l.get("event_name")
        if ev in ("TransferSingle", "TransferBatch"):
            from_a = ("0x" + l["topic2"][-40:]).lower() if l.get("topic2") else ""
            if from_a == wallet_lower:
                wallet_sold_shares = True
                break

    for l in logs:
        ev = l.get("event_name")
        block_number = l["block_number"]
        log_index = l["log_index"]
        contract_addr = l["contract_address"]
        data_hex = l["data"].removeprefix("0x") if l.get("data") else ""
        data_bytes = bytes.fromhex(data_hex) if data_hex else b""

        # ERC-20 (USDC.e / pUSD)
        if ev == "Transfer" and contract_addr in COLLATERAL_TOKENS:
            from_a = ("0x" + l["topic1"][-40:]).lower() if l.get("topic1") else ""
            to_a = ("0x" + l["topic2"][-40:]).lower() if l.get("topic2") else ""
            try:
                (amount,) = decode(["uint256"], data_bytes)
            except DecodingError, ValueError, TypeError:
                amount = int(data_hex, 16) if data_hex else 0

            if amount == 0:
                continue

            # Пропуск переводов самому себе (сальдо 0)
            if from_a == wallet_lower and to_a == wallet_lower:
                continue

            if to_a == wallet_lower:
                if has_exchange:
                    op = "TRADE_SELL" if wallet_sold_shares else "TRADE_BUY"
                elif has_merge:
                    op = "MERGE"
                elif has_redeem:
                    op = "REDEEM"
                elif has_onramp:
                    op = "WRAP"
                elif has_offramp:
                    op = "UNWRAP"
                else:
                    op = "TRANSFER_IN"

                balance_changes.append(
                    {
                        "wallet": Web3.to_checksum_address(wallet),
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC20",
                        "token_address": contract_addr,
                        "token_id": "0",
                        "amount_delta": int(amount),
                        "counterparty": Web3.to_checksum_address(from_a)
                        if from_a
                        else None,
                        "details": {**order_details, "source_log": ev},
                    }
                )

            elif from_a == wallet_lower:
                if has_exchange:
                    op = "TRADE_BUY"
                elif has_split:
                    op = "SPLIT"
                elif has_onramp:
                    op = "WRAP"
                elif has_offramp:
                    op = "UNWRAP"
                else:
                    op = "TRANSFER_OUT"

                balance_changes.append(
                    {
                        "wallet": Web3.to_checksum_address(wallet),
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC20",
                        "token_address": contract_addr,
                        "token_id": "0",
                        "amount_delta": -int(amount),
                        "counterparty": Web3.to_checksum_address(to_a)
                        if to_a
                        else None,
                        "details": {**order_details, "source_log": ev},
                    }
                )

        # ERC-1155 TransferSingle (CTF)
        elif ev == "TransferSingle" and contract_addr == CTF:
            from_a = ("0x" + l["topic2"][-40:]).lower() if l.get("topic2") else ""
            to_a = ("0x" + l["topic3"][-40:]).lower() if l.get("topic3") else ""
            try:
                token_id, value = decode(["uint256", "uint256"], data_bytes)
            except DecodingError, ValueError, TypeError:
                token_id = int(data_hex[:64], 16) if len(data_hex) >= 64 else 0
                value = int(data_hex[64:128], 16) if len(data_hex) >= 128 else 0

            if value == 0:
                continue

            # Пропуск переводов самому себе
            if from_a == wallet_lower and to_a == wallet_lower:
                continue

            token_id_str = str(int(token_id))

            if to_a == wallet_lower:
                if has_exchange:
                    op = "TRADE_BUY"
                elif has_split:
                    op = "SPLIT"
                else:
                    op = "TRANSFER_IN"

                balance_changes.append(
                    {
                        "wallet": Web3.to_checksum_address(wallet),
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC1155",
                        "token_address": contract_addr,
                        "token_id": token_id_str,
                        "amount_delta": int(value),
                        "counterparty": Web3.to_checksum_address(from_a)
                        if from_a
                        else None,
                        "details": {**order_details, "source_log": ev},
                    }
                )

            elif from_a == wallet_lower:
                if has_exchange:
                    op = "TRADE_SELL"
                elif has_merge:
                    op = "MERGE"
                elif has_redeem:
                    op = "REDEEM"
                else:
                    op = "TRANSFER_OUT"

                balance_changes.append(
                    {
                        "wallet": Web3.to_checksum_address(wallet),
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC1155",
                        "token_address": contract_addr,
                        "token_id": token_id_str,
                        "amount_delta": -int(value),
                        "counterparty": Web3.to_checksum_address(to_a)
                        if to_a
                        else None,
                        "details": {**order_details, "source_log": ev},
                    }
                )

        # ERC-1155 TransferBatch (CTF)
        elif ev == "TransferBatch" and contract_addr == CTF:
            from_a = ("0x" + l["topic2"][-40:]).lower() if l.get("topic2") else ""
            to_a = ("0x" + l["topic3"][-40:]).lower() if l.get("topic3") else ""
            try:
                ids, vals = decode(["uint256[]", "uint256[]"], data_bytes)
            except (DecodingError, ValueError, TypeError) as e:
                logger.warning(f"Error decoding TransferBatch in {tx_hash}: {e}")
                continue

            # Пропуск переводов самому себе
            if from_a == wallet_lower and to_a == wallet_lower:
                continue

            for token_id, value in zip(ids, vals):
                if value == 0:
                    continue

                token_id_str = str(int(token_id))

                if to_a == wallet_lower:
                    op = (
                        "SPLIT"
                        if has_split
                        else ("TRADE_BUY" if has_exchange else "TRANSFER_IN")
                    )
                    balance_changes.append(
                        {
                            "wallet": Web3.to_checksum_address(wallet),
                            "block_number": block_number,
                            "transaction_hash": tx_hash,
                            "log_index": log_index,
                            "operation_type": op,
                            "token_type": "ERC1155",
                            "token_address": contract_addr,
                            "token_id": token_id_str,
                            "amount_delta": int(value),
                            "counterparty": Web3.to_checksum_address(from_a)
                            if from_a
                            else None,
                            "details": {**order_details, "source_log": ev},
                        }
                    )

                elif from_a == wallet_lower:
                    op = (
                        "MERGE"
                        if has_merge
                        else (
                            "REDEEM"
                            if has_redeem
                            else ("TRADE_SELL" if has_exchange else "TRANSFER_OUT")
                        )
                    )
                    balance_changes.append(
                        {
                            "wallet": Web3.to_checksum_address(wallet),
                            "block_number": block_number,
                            "transaction_hash": tx_hash,
                            "log_index": log_index,
                            "operation_type": op,
                            "token_type": "ERC1155",
                            "token_address": contract_addr,
                            "token_id": token_id_str,
                            "amount_delta": -int(value),
                            "counterparty": Web3.to_checksum_address(to_a)
                            if to_a
                            else None,
                            "details": {**order_details, "source_log": ev},
                        }
                    )

    return balance_changes


class TransactionNormalizer:
    def __init__(self, wallet: str | None = None):
        self.wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
        self.checkpoint_id = f"normalizer_{self.wallet.lower()}"

    async def process_range(self, from_block: int, to_block: int) -> int:
        rows = (
            await RawLog.filter(
                block_number__gte=from_block,
                block_number__lte=to_block,
            )
            .order_by("block_number", "transaction_hash", "log_index")
            .values(
                "block_number",
                "transaction_hash",
                "log_index",
                "contract_address",
                "event_name",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
                "data",
            )
        )

        if not rows:
            return 0

        tx_logs = defaultdict(list)
        for r in rows:
            tx_logs[r["transaction_hash"]].append(r)

        all_changes: list[dict[str, Any]] = []
        for tx_hash, logs in tx_logs.items():
            changes = normalize_transaction(tx_hash, logs, self.wallet)
            all_changes.extend(changes)

        if all_changes:
            inserted = await insert_balance_changes(all_changes)
            return inserted
        return 0

    async def process_all(self, chunk_blocks: int = 50000) -> int:
        from .db import get_checkpoint, save_checkpoint

        latest_log = await RawLog.all().order_by("-block_number").first()
        max_raw_block = latest_log.block_number if latest_log else None

        if max_raw_block is None:
            logger.info("No raw logs found to normalize.")
            return 0

        last_checkpoint = await get_checkpoint(
            self.checkpoint_id, default=settings.start_block
        )
        current_start = (
            last_checkpoint
            if last_checkpoint == settings.start_block
            else last_checkpoint + 1
        )

        if current_start > max_raw_block:
            logger.info("All transactions are already normalized up to date.")
            return 0

        logger.info(
            f"Starting normalization for {self.wallet} from block {current_start:,} to {max_raw_block:,} "
            f"({max_raw_block - current_start + 1:,} blocks)..."
        )

        total_changes = 0
        while current_start <= max_raw_block:
            chunk_end = min(current_start + chunk_blocks - 1, max_raw_block)
            cnt = await self.process_range(current_start, chunk_end)
            await save_checkpoint(self.checkpoint_id, chunk_end)
            total_changes += cnt
            if cnt > 0:
                logger.info(
                    f"Normalized blocks {current_start:,}..{chunk_end:,}: {cnt} balance changes generated."
                )
            current_start = chunk_end + 1

        return total_changes
