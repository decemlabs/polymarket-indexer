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
from .db import CheckpointTracker, get_checkpoint, insert_raw_logs
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


DEFAULT_CHUNK_SIZE = 25_000
FALLBACK_START_BLOCK = 40_000_000


class BlockchainScanner:
    def __init__(
        self,
        rpc: MultiRpcClient | None = None,
        wallet: str | None = None,
        concurrency: int | None = None,
        initial_chunk_size: int | None = None,
        min_chunk_size: int = 500,
        max_chunk_size: int | None = None,
    ):
        self.rpc = rpc or rpc_client
        self.wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)
        raw = self.wallet[2:].lower()
        self.wallet_topic = "0x" + raw.rjust(64, "0")
        self.checkpoint_id = f"wallet_{self.wallet.lower()}"
        self.concurrency = max(1, concurrency or settings.scanner_concurrency)
        self.chunk_size = initial_chunk_size or settings.chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size or settings.max_chunk_size
        self.queries = build_queries(self.wallet_topic)
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency * len(self.queries),
            thread_name_prefix="scanner",
        )

    def close(self) -> None:
        self._executor.shutdown(wait=False)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def scan_range(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
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

    async def get_start_block(self) -> int:
        saved = await get_checkpoint(self.checkpoint_id, default=0)
        if saved > 0:
            return saved

        if settings.start_block is not None and settings.start_block > 0:
            return settings.start_block

        logger.info(f"Auto-detecting first active block for {self.wallet}...")
        first_block = await asyncio.to_thread(
            self.rpc.find_first_wallet_block, self.wallet, FALLBACK_START_BLOCK
        )
        logger.info(f"Start block for {self.wallet}: {first_block:,}")
        return first_block

    async def run(
        self,
        target_block: int | None = None,
        max_chunks: int | None = None,
    ) -> int:
        latest_network_block = self.rpc.get_latest_block()
        if target_block is None or target_block > latest_network_block:
            target_block = latest_network_block

        start_block = await self.get_start_block()
        saved_checkpoint = await get_checkpoint(self.checkpoint_id, default=0)
        current_start = saved_checkpoint + 1 if saved_checkpoint > 0 else start_block
        initial_start = current_start

        if current_start > target_block:
            logger.info(
                f"Already up to date: checkpoint {current_start - 1:,} >= target {target_block:,}"
            )
            return 0

        total_to_scan = target_block - current_start + 1
        logger.info(
            f"Starting scan for {self.wallet} from block {current_start:,} to {target_block:,} "
            f"({total_to_scan:,} blocks remaining, concurrency={self.concurrency}, chunk_size={self.chunk_size:,})"
        )

        tracker = CheckpointTracker(
            initial_checkpoint=current_start - 1, checkpoint_id=self.checkpoint_id
        )
        queue: asyncio.PriorityQueue[tuple[int, int]] = asyncio.PriorityQueue()
        stop_event = asyncio.Event()
        stats_lock = asyncio.Lock()

        next_block_to_enqueue = current_start
        chunks_enqueued = 0
        chunks_processed = 0
        total_inserted = 0
        active_tasks = 0
        t0 = time.time()

        async def producer() -> None:
            nonlocal next_block_to_enqueue, chunks_enqueued
            while next_block_to_enqueue <= target_block and not stop_event.is_set():
                if max_chunks is not None and chunks_enqueued >= max_chunks:
                    break
                if queue.qsize() < self.concurrency * 3:
                    chunk_end = min(
                        next_block_to_enqueue + self.chunk_size - 1, target_block
                    )
                    await queue.put((next_block_to_enqueue, chunk_end))
                    chunks_enqueued += 1
                    next_block_to_enqueue = chunk_end + 1
                else:
                    await asyncio.sleep(0.02)

        async def worker(worker_id: int) -> None:
            nonlocal active_tasks, total_inserted, chunks_processed
            while not stop_event.is_set():
                # Check if all work is done
                work_finished = next_block_to_enqueue > target_block or (
                    max_chunks is not None and chunks_enqueued >= max_chunks
                )
                if work_finished and queue.empty() and active_tasks == 0:
                    break

                try:
                    from_b, to_b = await asyncio.wait_for(queue.get(), timeout=0.2)
                except TimeoutError:
                    continue

                async with stats_lock:
                    active_tasks += 1

                try:
                    t_chunk = time.time()
                    logs = await asyncio.to_thread(self.scan_range, from_b, to_b)
                    inserted = await insert_raw_logs(logs)
                    await tracker.mark_completed(from_b, to_b)

                    async with stats_lock:
                        total_inserted += inserted
                        chunks_processed += 1
                        cur_chunks = chunks_processed

                    dt_chunk = time.time() - t_chunk
                    cp_display = tracker.last_saved_checkpoint
                    blocks_done = max(0, cp_display - initial_start + 1)
                    progress_pct = (
                        (blocks_done / total_to_scan * 100)
                        if total_to_scan > 0
                        else 100.0
                    )
                    elapsed = time.time() - t0
                    speed = (blocks_done / elapsed) if elapsed > 0 else 0

                    logger.info(
                        f"[Worker-{worker_id}] Blocks {from_b:,}..{to_b:,} "
                        f"({to_b - from_b + 1:,} blk in {dt_chunk:.2f}s) | "
                        f"Logs: {len(logs)} (new: {inserted}) | "
                        f"Checkpoint: {cp_display:,}/{target_block:,} ({progress_pct:.2f}%) | "
                        f"Speed: {speed:.0f} blk/s"
                    )

                    if max_chunks is not None and cur_chunks >= max_chunks:
                        stop_event.set()

                except RangeLimitError as e:
                    chunk_len = to_b - from_b + 1
                    if chunk_len > self.min_chunk_size:
                        mid = (from_b + to_b) // 2
                        logger.warning(
                            f"[Worker-{worker_id}] Range limit exceeded on {from_b:,}..{to_b:,}. "
                            f"Splitting into {from_b:,}..{mid:,} and {mid + 1:,}..{to_b:,}. Error: {e}"
                        )
                        self.chunk_size = max(self.chunk_size // 2, self.min_chunk_size)
                        await queue.put((from_b, mid))
                        await queue.put((mid + 1, to_b))
                    else:
                        logger.error(
                            f"[Worker-{worker_id}] Cannot reduce chunk below min_chunk_size ({self.min_chunk_size}): {e}"
                        )
                        raise

                except Exception as e:  # noqa: BLE001
                    logger.error(
                        f"[Worker-{worker_id}] Error scanning blocks {from_b:,}..{to_b:,}: {e}. Retrying with backoff..."
                    )
                    self.rpc.rotate_node()
                    await asyncio.sleep(2.0)
                    await queue.put((from_b, to_b))

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
            logger.info(
                "Scan cancelled by user. Waiting for active tasks to finalize..."
            )
            stop_event.set()
            producer_task.cancel()
            for w in worker_tasks:
                w.cancel()
            await asyncio.gather(producer_task, *worker_tasks, return_exceptions=True)
            raise

        elapsed = time.time() - t0
        blocks_done = max(0, tracker.last_saved_checkpoint - initial_start + 1)
        speed = blocks_done / elapsed if elapsed > 0 else 0
        logger.info(
            f"Scanner finished: processed {chunks_processed} chunks ({blocks_done:,} blocks) in {elapsed:.1f}s "
            f"({speed:.0f} blk/s). Total logs processed: {total_inserted}."
        )
        stats = self.rpc.get_stats()
        stat_strs = [
            f"{url.split('//')[1].split('/')[0]}: {s['total']} reqs ({s['errors']} err)"
            for url, s in stats.items()
        ]
        logger.info(f"RPC Load Balancing: {' | '.join(stat_strs)}")
        return total_inserted

    async def run_live(self, poll_interval: float = 3.0) -> None:
        logger.info(
            f"Starting continuous live indexing for {self.wallet} (poll interval: {poll_interval}s)..."
        )
        while True:
            try:
                latest = self.rpc.get_latest_block()
                start_block = await self.get_start_block()
                last_checkpoint = await get_checkpoint(
                    self.checkpoint_id, default=start_block
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
