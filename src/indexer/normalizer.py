import asyncio
import logging
import time
from collections import defaultdict
from typing import Any

from eth_abi.abi import decode
from eth_abi.exceptions import DecodingError
from tortoise import Tortoise
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

logger = logging.getLogger(__name__)

_CHECKSUM_CACHE: dict[str, str] = {}


def fast_to_checksum_address(addr: str | None) -> str | None:
    if not addr:
        return None
    res = _CHECKSUM_CACHE.get(addr)
    if res is None:
        res = Web3.to_checksum_address(addr)
        if len(_CHECKSUM_CACHE) < 100_000:
            _CHECKSUM_CACHE[addr] = res
    return res


def normalize_transaction(
    tx_hash: str,
    logs: list[dict[str, Any]],
    wallet: str,
    wallet_lower: str | None = None,
) -> list[dict[str, Any]]:
    if wallet_lower is None:
        wallet_lower = wallet.lower()
    wallet_checksum = fast_to_checksum_address(wallet)
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
            if len(data_bytes) >= 32:
                amount = int.from_bytes(data_bytes[:32], "big")
            else:
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
                        "wallet": wallet_checksum,
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC20",
                        "token_address": contract_addr,
                        "token_id": "0",
                        "amount_delta": int(amount),
                        "counterparty": fast_to_checksum_address(from_a)
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
                        "wallet": wallet_checksum,
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC20",
                        "token_address": contract_addr,
                        "token_id": "0",
                        "amount_delta": -int(amount),
                        "counterparty": fast_to_checksum_address(to_a)
                        if to_a
                        else None,
                        "details": {**order_details, "source_log": ev},
                    }
                )

        # ERC-1155 TransferSingle (CTF)
        elif ev == "TransferSingle" and contract_addr == CTF:
            from_a = ("0x" + l["topic2"][-40:]).lower() if l.get("topic2") else ""
            to_a = ("0x" + l["topic3"][-40:]).lower() if l.get("topic3") else ""
            if len(data_bytes) >= 64:
                token_id = int.from_bytes(data_bytes[:32], "big")
                value = int.from_bytes(data_bytes[32:64], "big")
            else:
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
                        "wallet": wallet_checksum,
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC1155",
                        "token_address": contract_addr,
                        "token_id": token_id_str,
                        "amount_delta": int(value),
                        "counterparty": fast_to_checksum_address(from_a)
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
                        "wallet": wallet_checksum,
                        "block_number": block_number,
                        "transaction_hash": tx_hash,
                        "log_index": log_index,
                        "operation_type": op,
                        "token_type": "ERC1155",
                        "token_address": contract_addr,
                        "token_id": token_id_str,
                        "amount_delta": -int(value),
                        "counterparty": fast_to_checksum_address(to_a)
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
                            "wallet": wallet_checksum,
                            "block_number": block_number,
                            "transaction_hash": tx_hash,
                            "log_index": log_index,
                            "operation_type": op,
                            "token_type": "ERC1155",
                            "token_address": contract_addr,
                            "token_id": token_id_str,
                            "amount_delta": int(value),
                            "counterparty": fast_to_checksum_address(from_a)
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
                            "wallet": wallet_checksum,
                            "block_number": block_number,
                            "transaction_hash": tx_hash,
                            "log_index": log_index,
                            "operation_type": op,
                            "token_type": "ERC1155",
                            "token_address": contract_addr,
                            "token_id": token_id_str,
                            "amount_delta": -int(value),
                            "counterparty": fast_to_checksum_address(to_a)
                            if to_a
                            else None,
                            "details": {**order_details, "source_log": ev},
                        }
                    )

    return balance_changes


class TransactionNormalizer:
    def __init__(
        self,
        wallet: str | None = None,
        concurrency: int | None = None,
        chunk_size: int | None = None,
    ):
        self.wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
        self.wallet_lower = self.wallet.lower()
        self.checkpoint_id = f"normalizer_{self.wallet_lower}"
        self.concurrency = max(1, concurrency or settings.normalizer_concurrency)
        self.chunk_size = chunk_size or settings.normalizer_chunk_size
        self._write_lock = asyncio.Lock()

    async def process_range(self, from_block: int, to_block: int) -> int:
        conn = Tortoise.get_connection("default")
        if conn.capabilities.dialect == "sqlite":
            rows = await conn.execute_query_dict(
                """
                SELECT block_number, transaction_hash, log_index, contract_address,
                       event_name, topic0, topic1, topic2, topic3, data
                FROM raw_logs
                WHERE block_number >= ? AND block_number <= ?
                ORDER BY block_number, transaction_hash, log_index
                """,
                [from_block, to_block],
            )
        else:
            rows = await conn.execute_query_dict(
                """
                SELECT block_number, transaction_hash, log_index, contract_address,
                       event_name, topic0, topic1, topic2, topic3, data
                FROM raw_logs
                WHERE block_number >= $1 AND block_number <= $2
                ORDER BY block_number, transaction_hash, log_index
                """,
                [from_block, to_block],
            )

        if not rows:
            return 0

        tx_logs = defaultdict(list)
        for r in rows:
            tx_logs[r["transaction_hash"]].append(r)

        all_changes: list[dict[str, Any]] = []
        for tx_hash, logs in tx_logs.items():
            changes = normalize_transaction(
                tx_hash, logs, self.wallet, self.wallet_lower
            )
            all_changes.extend(changes)

        if all_changes:
            async with self._write_lock:
                inserted = await insert_balance_changes(all_changes)
                return inserted
        return 0

    async def process_all(
        self,
        chunk_blocks: int | None = None,
        target_block: int | None = None,
    ) -> int:
        from .db import CheckpointTracker, get_checkpoint

        conn = Tortoise.get_connection("default")
        bounds = await conn.execute_query_dict(
            "SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM raw_logs;"
        )
        if not bounds or not bounds[0] or bounds[0]["max_b"] is None:
            logger.info("No raw logs found to normalize.")
            return 0

        max_raw_block = bounds[0]["max_b"]
        if target_block is not None:
            max_raw_block = min(max_raw_block, target_block)

        saved_checkpoint = await get_checkpoint(self.checkpoint_id, default=0)
        if saved_checkpoint > 0:
            current_start = saved_checkpoint + 1
        else:
            current_start = bounds[0]["min_b"] or 0

        if current_start > max_raw_block:
            logger.info("All transactions are already normalized up to date.")
            return 0

        effective_chunk_size = chunk_blocks or self.chunk_size
        total_to_process = max_raw_block - current_start + 1

        logger.info(
            f"Starting parallel normalization for {self.wallet} "
            f"from block {current_start:,} to {max_raw_block:,} "
            f"({total_to_process:,} blocks, concurrency={self.concurrency}, chunk_size={effective_chunk_size:,})..."
        )

        tracker = CheckpointTracker(
            initial_checkpoint=current_start - 1,
            checkpoint_id=self.checkpoint_id,
        )

        queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue(
            maxsize=self.concurrency * 4
        )
        stop_event = asyncio.Event()

        active_tasks = 0
        total_changes = 0
        chunks_processed = 0
        stats_lock = asyncio.Lock()
        t0 = time.time()

        next_block = current_start

        async def producer() -> None:
            nonlocal next_block
            while not stop_event.is_set() and next_block <= max_raw_block:
                chunk_end = min(next_block + effective_chunk_size - 1, max_raw_block)
                await queue.put((next_block, chunk_end))
                next_block = chunk_end + 1
                await asyncio.sleep(0.01)

        async def worker(worker_id: int) -> None:
            nonlocal active_tasks, total_changes, chunks_processed
            while not stop_event.is_set():
                if next_block > max_raw_block and queue.empty() and active_tasks == 0:
                    break

                try:
                    from_b, to_b = await asyncio.wait_for(queue.get(), timeout=0.2)
                except TimeoutError:
                    continue

                async with stats_lock:
                    active_tasks += 1

                try:
                    t_chunk = time.time()
                    cnt = await self.process_range(from_b, to_b)
                    await tracker.mark_completed(from_b, to_b)

                    async with stats_lock:
                        total_changes += cnt
                        chunks_processed += 1

                    dt_chunk = time.time() - t_chunk
                    cp_display = tracker.last_saved_checkpoint
                    blocks_done = max(0, cp_display - current_start + 1)
                    progress_pct = (
                        (blocks_done / total_to_process * 100)
                        if total_to_process > 0
                        else 100.0
                    )
                    elapsed = time.time() - t0
                    speed = (blocks_done / elapsed) if elapsed > 0 else 0

                    if cnt > 0:
                        logger.info(
                            f"[Normalizer-{worker_id}] Blocks {from_b:,}..{to_b:,} "
                            f"({to_b - from_b + 1:,} blk in {dt_chunk:.2f}s) | "
                            f"Changes: {cnt:,} | "
                            f"Checkpoint: {cp_display:,}/{max_raw_block:,} ({progress_pct:.2f}%) | "
                            f"Speed: {speed:.0f} blk/s"
                        )
                    else:
                        logger.debug(
                            f"[Normalizer-{worker_id}] Blocks {from_b:,}..{to_b:,}: 0 changes."
                        )

                except Exception as e:  # noqa: BLE001
                    logger.error(
                        f"[Normalizer-{worker_id}] Error normalizing blocks {from_b:,}..{to_b:,}: {e}"
                    )
                    await queue.put((from_b, to_b))
                    await asyncio.sleep(1.0)
                finally:
                    queue.task_done()
                    async with stats_lock:
                        active_tasks -= 1

        producer_task = asyncio.create_task(producer())
        worker_tasks = [
            asyncio.create_task(worker(i + 1)) for i in range(self.concurrency)
        ]

        try:
            await asyncio.gather(producer_task, *worker_tasks)
        except KeyboardInterrupt, asyncio.CancelledError:
            logger.info("Normalization cancelled. Stopping workers...")
            stop_event.set()
            producer_task.cancel()
            for w in worker_tasks:
                w.cancel()
            await asyncio.gather(producer_task, *worker_tasks, return_exceptions=True)
            raise

        elapsed = time.time() - t0
        blocks_done = max(0, tracker.last_saved_checkpoint - current_start + 1)
        speed = blocks_done / elapsed if elapsed > 0 else 0
        logger.info(
            f"Normalization finished: processed {chunks_processed} chunks ({blocks_done:,} blocks) in {elapsed:.1f}s "
            f"({speed:.0f} blk/s). Total balance changes generated: {total_changes:,}."
        )
        return total_changes
