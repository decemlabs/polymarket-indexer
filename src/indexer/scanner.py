import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Self

from web3 import Web3

from .config import settings
from .contracts import (
    CTF,
    EXCHANGE_CONTRACTS,
    PUSD,
    TOPIC_ORDER_FILLED_V1,
    TOPIC_ORDER_FILLED_V2,
    TOPIC_ORDERS_MATCHED_V1,
    TOPIC_ORDERS_MATCHED_V2,
    TOPIC_PAYOUT_REDEMPTION,
    TOPIC_POSITION_SPLIT,
    TOPIC_POSITIONS_MERGE,
    TOPIC_TRANSFER_BATCH,
    TOPIC_TRANSFER_ERC20,
    TOPIC_TRANSFER_SINGLE,
    USDC_E,
)
from .db import get_checkpoint, insert_raw_logs, save_checkpoint
from .rpc import MultiRpcClient, RangeLimitError, rpc_client

logger = logging.getLogger(__name__)

TOPIC_TO_NAME = {
    TOPIC_TRANSFER_ERC20.lower(): "Transfer",
    TOPIC_TRANSFER_SINGLE.lower(): "TransferSingle",
    TOPIC_TRANSFER_BATCH.lower(): "TransferBatch",
    TOPIC_POSITION_SPLIT.lower(): "PositionSplit",
    TOPIC_POSITIONS_MERGE.lower(): "PositionsMerge",
    TOPIC_PAYOUT_REDEMPTION.lower(): "PayoutRedemption",
    TOPIC_ORDER_FILLED_V1.lower(): "OrderFilledV1",
    TOPIC_ORDER_FILLED_V2.lower(): "OrderFilledV2",
    TOPIC_ORDERS_MATCHED_V1.lower(): "OrdersMatchedV1",
    TOPIC_ORDERS_MATCHED_V2.lower(): "OrdersMatchedV2",
}


def to_hex_str(val: Any) -> str:
    """Converts a bytes/HexBytes/string value into a 0x-prefixed hex string."""
    if val is None:
        return ""
    if hasattr(val, "hex"):
        h = val.hex()
        return h if h.startswith("0x") else f"0x{h}"
    s = str(val)
    return s if s.startswith("0x") else f"0x{s}"


def format_raw_log(log: dict[str, Any]) -> dict[str, Any]:
    topics = log.get("topics", [])
    topic_hexes = [to_hex_str(t).lower() for t in topics]
    topic0 = topic_hexes[0] if len(topic_hexes) > 0 else ""
    event_name = TOPIC_TO_NAME.get(topic0, "Unknown")

    data_str = to_hex_str(log.get("data", ""))
    block_hash_str = to_hex_str(log.get("blockHash", ""))
    tx_hash_str = to_hex_str(log.get("transactionHash", ""))

    return {
        "block_number": int(log["blockNumber"]),
        "block_hash": block_hash_str,
        "transaction_hash": tx_hash_str,
        "transaction_index": int(log["transactionIndex"]),
        "log_index": int(log["logIndex"]),
        "contract_address": Web3.to_checksum_address(log["address"]),
        "event_name": event_name,
        "topic0": topic0,
        "topic1": topic_hexes[1] if len(topic_hexes) > 1 else None,
        "topic2": topic_hexes[2] if len(topic_hexes) > 2 else None,
        "topic3": topic_hexes[3] if len(topic_hexes) > 3 else None,
        "data": data_str,
    }


def build_queries(wallet_topic: str) -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            "USDC_IN",
            {
                "address": [USDC_E, PUSD],
                "topics": [TOPIC_TRANSFER_ERC20, None, wallet_topic],
            },
        ),
        (
            "USDC_OUT",
            {
                "address": [USDC_E, PUSD],
                "topics": [TOPIC_TRANSFER_ERC20, wallet_topic],
            },
        ),
        (
            "CTF_SINGLE_IN",
            {
                "address": CTF,
                "topics": [TOPIC_TRANSFER_SINGLE, None, None, wallet_topic],
            },
        ),
        (
            "CTF_SINGLE_OUT",
            {
                "address": CTF,
                "topics": [TOPIC_TRANSFER_SINGLE, None, wallet_topic],
            },
        ),
        (
            "CTF_BATCH_IN",
            {
                "address": CTF,
                "topics": [TOPIC_TRANSFER_BATCH, None, None, wallet_topic],
            },
        ),
        (
            "CTF_BATCH_OUT",
            {
                "address": CTF,
                "topics": [TOPIC_TRANSFER_BATCH, None, wallet_topic],
            },
        ),
        (
            "CTF_OPERATIONS",
            {
                "address": CTF,
                "topics": [
                    [
                        TOPIC_POSITION_SPLIT,
                        TOPIC_POSITIONS_MERGE,
                        TOPIC_PAYOUT_REDEMPTION,
                    ],
                    wallet_topic,
                ],
            },
        ),
        (
            "EXCHANGE_MAKER",
            {
                "address": list(EXCHANGE_CONTRACTS),
                "topics": [
                    [
                        TOPIC_ORDER_FILLED_V1,
                        TOPIC_ORDER_FILLED_V2,
                        TOPIC_ORDERS_MATCHED_V1,
                        TOPIC_ORDERS_MATCHED_V2,
                    ],
                    None,
                    wallet_topic,
                ],
            },
        ),
        (
            "EXCHANGE_TAKER",
            {
                "address": list(EXCHANGE_CONTRACTS),
                "topics": [
                    [TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2],
                    None,
                    None,
                    wallet_topic,
                ],
            },
        ),
    ]


class BlockchainScanner:
    def __init__(
        self,
        rpc: MultiRpcClient | None = None,
        wallet: str | None = None,
        initial_chunk_size: int | None = None,
        min_chunk_size: int = 500,
        max_chunk_size: int = 200000,
    ):
        self.rpc = rpc or rpc_client
        self.wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
        self.wallet_topic = settings.wallet_topic
        self.checkpoint_id = f"wallet_{self.wallet.lower()}"
        self.chunk_size = initial_chunk_size or settings.chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.queries = build_queries(self.wallet_topic)
        self._executor = ThreadPoolExecutor(
            max_workers=len(self.queries), thread_name_prefix="scanner"
        )

    def close(self) -> None:
        """Shuts down the thread pool executor."""
        self._executor.shutdown(wait=False)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def scan_range(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        """Queries all 9 targeted event filters concurrently and deduplicates logs."""
        all_logs: dict[tuple[str, int], dict[str, Any]] = {}

        def fetch_query(item: tuple[str, dict[str, Any]]) -> list[dict[str, Any]]:
            _name, q = item
            q_copy = dict(q)
            q_copy["fromBlock"] = from_block
            q_copy["toBlock"] = to_block
            return self.rpc.get_logs(q_copy)

        query_results = self._executor.map(fetch_query, self.queries)

        for logs in query_results:
            for log in logs:
                formatted = format_raw_log(log)
                key = (formatted["transaction_hash"], formatted["log_index"])
                all_logs[key] = formatted

        return sorted(
            all_logs.values(),
            key=lambda x: (x["block_number"], x["log_index"]),
        )

    async def run(
        self,
        target_block: int | None = None,
        max_chunks: int | None = None,
    ) -> int:
        """
        Runs the scanner starting from the last saved checkpoint up to target_block.
        Returns the total number of newly inserted logs.
        """
        latest_network_block = self.rpc.get_latest_block()
        if target_block is None or target_block > latest_network_block:
            target_block = latest_network_block

        last_checkpoint = await get_checkpoint(
            self.checkpoint_id, default=settings.start_block
        )
        current_start = (
            last_checkpoint
            if last_checkpoint == settings.start_block
            else last_checkpoint + 1
        )

        total_to_scan = max(0, target_block - current_start + 1)
        logger.info(
            f"Starting scan for {self.wallet} from block {current_start} to {target_block} "
            f"({total_to_scan:,} blocks remaining, initial chunk_size={self.chunk_size})"
        )

        total_inserted = 0
        chunks_processed = 0
        t0 = time.time()

        while current_start <= target_block:
            if max_chunks is not None and chunks_processed >= max_chunks:
                logger.info(f"Reached maximum chunk limit of {max_chunks}. Stopping.")
                break

            chunk_end = min(current_start + self.chunk_size - 1, target_block)

            try:
                t_chunk = time.time()
                # Run sync thread pool query in executor to not block asyncio loop
                logs = await asyncio.to_thread(
                    self.scan_range, current_start, chunk_end
                )
                inserted = await insert_raw_logs(logs)
                await save_checkpoint(self.checkpoint_id, chunk_end)

                total_inserted += inserted
                chunks_processed += 1
                dt_chunk = time.time() - t_chunk

                progress_pct = (
                    (chunk_end - last_checkpoint) / total_to_scan * 100
                    if total_to_scan > 0
                    else 100.0
                )
                logger.info(
                    f"[{chunk_end:,}/{target_block:,} ({progress_pct:.2f}%)] "
                    f"Blocks {current_start:,}..{chunk_end:,} ({chunk_end - current_start + 1} blk in {dt_chunk:.2f}s) "
                    f"| Logs found: {len(logs)} | Total new: {total_inserted} "
                    f"| Node: {self.rpc.current_node.url}"
                )

                if self.chunk_size < self.max_chunk_size:
                    self.chunk_size = min(
                        int(self.chunk_size * 1.2), self.max_chunk_size
                    )

                current_start = chunk_end + 1

            except RangeLimitError as e:
                new_chunk = max(self.chunk_size // 2, self.min_chunk_size)
                logger.warning(
                    f"Range limit exceeded on block range {current_start}..{chunk_end}. "
                    f"Reducing chunk size {self.chunk_size} -> {new_chunk}. Error: {e}"
                )
                self.chunk_size = new_chunk
                await asyncio.sleep(0.5)

            except Exception as e:  # noqa: BLE001
                logger.error(
                    f"Unexpected error scanning blocks {current_start}..{chunk_end}: {e}. Retrying with backoff..."
                )
                self.rpc.rotate_node()
                await asyncio.sleep(2.0)

        elapsed = time.time() - t0
        blocks_done = current_start - (last_checkpoint + 1)
        speed = blocks_done / elapsed if elapsed > 0 else 0
        logger.info(
            f"Scanner finished: processed {chunks_processed} chunks ({blocks_done:,} blocks) in {elapsed:.1f}s "
            f"({speed:.0f} blk/s). Total logs processed: {total_inserted}."
        )
        return total_inserted

    async def run_live(self, poll_interval: float = 3.0) -> None:
        """
        Continuously polls for new Polygon blocks after catching up,
        indexing each new block as it is produced.
        """
        logger.info(
            f"Starting continuous live indexing for {self.wallet} (poll interval: {poll_interval}s)..."
        )
        while True:
            try:
                latest = self.rpc.get_latest_block()
                last_checkpoint = await get_checkpoint(
                    self.checkpoint_id, default=settings.start_block
                )
                if latest > last_checkpoint:
                    inserted = await self.run(target_block=latest)
                    if inserted > 0:
                        from .ledger import update_current_balances
                        from .normalizer import TransactionNormalizer

                        normalizer = TransactionNormalizer(wallet=self.wallet)
                        await normalizer.process_all()
                        await update_current_balances(wallet=self.wallet)
                else:
                    await asyncio.sleep(poll_interval)
            except KeyboardInterrupt:
                logger.info("Live indexing stopped by user.")
                break
            except Exception as e:  # noqa: BLE001
                logger.error(f"Error in live loop: {e}. Sleeping {poll_interval}s...")
                await asyncio.sleep(poll_interval)
