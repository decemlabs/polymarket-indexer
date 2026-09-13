import logging
import random
import threading
import time
from typing import Any, cast

import requests
from requests.adapters import HTTPAdapter
from web3 import Web3
from web3.exceptions import Web3Exception, Web3RPCError
from web3.middleware import ExtraDataToPOAMiddleware
from web3.types import BlockIdentifier, FilterParams

from .config import settings
from .contracts import ERC20_BALANCE_OF_ABI, ERC1155_BALANCE_OF_ABI

logger = logging.getLogger(__name__)


class RangeLimitError(Exception):
    pass


class RpcNode:
    def __init__(self, url: str, max_concurrent: int = 3):
        self.url = url
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=64,
            pool_maxsize=64,
            max_retries=1,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        self.w3 = Web3(
            Web3.HTTPProvider(url, session=session, request_kwargs={"timeout": 15})
        )
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.failure_count = 0
        self.last_failed_at = 0.0
        self.active_requests = 0
        self.max_concurrent = max_concurrent
        self.total_requests = 0
        self.total_errors = 0
        self.is_archive = True
        self.disabled = False

    def is_healthy(self, cooloff_seconds: float = 2.0) -> bool:
        if self.disabled:
            return False
        if self.failure_count == 0:
            return True
        return time.time() - self.last_failed_at > cooloff_seconds

    def record_success(self) -> None:
        self.failure_count = 0

    def record_failure(self) -> None:
        self.failure_count += 1
        self.total_errors += 1
        self.last_failed_at = time.time()


class MultiRpcClient:
    def __init__(self, urls: list[str] | None = None, max_concurrent_per_node: int = 3):
        if not urls:
            urls = settings.rpc_urls
        self.nodes = [
            RpcNode(url, max_concurrent=max_concurrent_per_node) for url in urls
        ]
        self._current_index = 0
        self._cond = threading.Condition()

    def acquire_node(self, exclude_urls: set[str] | None = None) -> RpcNode:
        """Acquires the least-loaded healthy node. Blocks if all nodes are at max capacity."""
        with self._cond:
            while True:
                # 1. First priority: enabled, healthy nodes not yet tried in this attempt
                candidates = [
                    n
                    for n in self.nodes
                    if not n.disabled
                    and n.is_healthy()
                    and (exclude_urls is None or n.url not in exclude_urls)
                ]
                # 2. Second priority: any enabled node not yet tried
                if not candidates:
                    candidates = [
                        n
                        for n in self.nodes
                        if not n.disabled
                        and (exclude_urls is None or n.url not in exclude_urls)
                    ]
                # 3. Third priority: all non-disabled nodes
                if not candidates:
                    candidates = [n for n in self.nodes if not n.disabled]
                if not candidates:
                    raise RuntimeError("All RPC nodes are permanently disabled!")

                # Pick candidates that are under max_concurrent
                avail = [n for n in candidates if n.active_requests < n.max_concurrent]
                if avail:
                    min_active = min(n.active_requests for n in avail)
                    best_candidates = [
                        n for n in avail if n.active_requests == min_active
                    ]
                    self._current_index = (self._current_index + 1) % len(
                        best_candidates
                    )
                    chosen = best_candidates[self._current_index]
                    chosen.active_requests += 1
                    chosen.total_requests += 1
                    return chosen

                # All nodes are at capacity, wait for one to finish
                self._cond.wait(timeout=0.1)

    def release_node(self, node: RpcNode) -> None:
        with self._cond:
            if node.active_requests > 0:
                node.active_requests -= 1
            self._cond.notify_all()

    def get_stats(self) -> dict[str, dict[str, Any]]:
        with self._cond:
            return {
                n.url: {
                    "active": n.active_requests,
                    "total": n.total_requests,
                    "errors": n.total_errors,
                    "disabled": n.disabled,
                }
                for n in self.nodes
            }

    def get_next_healthy_node(self) -> RpcNode:
        with self._cond:
            for i in range(len(self.nodes)):
                idx = (self._current_index + i) % len(self.nodes)
                if not self.nodes[idx].disabled and self.nodes[idx].is_healthy():
                    self._current_index = (idx + 1) % len(self.nodes)
                    return self.nodes[idx]
            return self.nodes[self._current_index % len(self.nodes)]

    @property
    def current_node(self) -> RpcNode:
        with self._cond:
            for i in range(len(self.nodes)):
                idx = (self._current_index + i) % len(self.nodes)
                if not self.nodes[idx].disabled and self.nodes[idx].is_healthy():
                    return self.nodes[idx]
            return self.nodes[self._current_index % len(self.nodes)]

    def rotate_node(self) -> RpcNode:
        with self._cond:
            old_node = self.nodes[self._current_index % len(self.nodes)]
            old_node.record_failure()
            self._current_index = (self._current_index + 1) % len(self.nodes)
            new_node = self.nodes[self._current_index]
            logger.warning(f"Rotated RPC node from {old_node.url} -> {new_node.url}")
            return new_node

    def get_latest_block(self) -> int:
        for _ in range(len(self.nodes)):
            node = self.current_node
            try:
                block_number = node.w3.eth.block_number
                node.record_success()
                return int(block_number)
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(f"Failed to get block number from {node.url}: {e}")
                self.rotate_node()
        raise RuntimeError("All RPC nodes failed to fetch latest block number")

    def get_transaction_count(
        self, wallet: str, block_identifier: BlockIdentifier = "latest"
    ) -> int:
        checksum_wallet = Web3.to_checksum_address(wallet)
        for _ in range(len(self.nodes)):
            node = self.current_node
            try:
                cnt = node.w3.eth.get_transaction_count(
                    checksum_wallet, block_identifier=block_identifier
                )
                node.record_success()
                return int(cnt)
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(
                    f"Error calling get_transaction_count on {node.url}: {e}"
                )
                self.rotate_node()
        raise RuntimeError(
            f"All RPC nodes failed calling get_transaction_count for {wallet}"
        )

    def find_first_wallet_block(
        self,
        wallet: str,
        min_block: int = 40_000_000,
        buffer_blocks: int = 5_000,
    ) -> int:
        latest = self.get_latest_block()
        if self.get_transaction_count(wallet, "latest") == 0:
            return min_block

        if self.get_transaction_count(wallet, min_block) > 0:
            return min_block

        low = min_block
        high = latest
        while low < high:
            mid = (low + high) // 2
            cnt = self.get_transaction_count(wallet, mid)
            if cnt == 0:
                low = mid + 1
            else:
                high = mid

        return max(min_block, low - buffer_blocks)

    def get_logs(
        self,
        filter_params: dict[str, Any],
        max_retries: int = 5,
        initial_backoff: float = 0.5,
    ) -> list[dict[str, Any]]:
        backoff = initial_backoff
        tried_urls: set[str] = set()

        for attempt in range(1, max_retries + 1):
            exclude = tried_urls if len(tried_urls) < len(self.nodes) else None
            node = self.acquire_node(exclude_urls=exclude)
            try:
                raw_logs = node.w3.eth.get_logs(cast(FilterParams, filter_params))
                node.record_success()
                return [dict(log) for log in raw_logs]
            except Web3RPCError as e:
                err_msg = str(e).lower()
                if any(
                    phrase in err_msg
                    for phrase in [
                        "maximum block range",
                        "range is over limit",
                        "size exceeded",
                        "block range is too large",
                        "query returned more than",
                    ]
                ):
                    raise RangeLimitError(
                        f"Block range exceeded on {node.url}: {e}"
                    ) from e

                if any(
                    phrase in err_msg
                    for phrase in [
                        "history has been pruned",
                        "pruned",
                        "failed to get logs for block",
                    ]
                ):
                    logger.warning(
                        f"Node {node.url} has pruned history: {e}. Disabling node."
                    )
                    node.disabled = True
                    tried_urls.add(node.url)
                    continue

                logger.warning(
                    f"RPC error on {node.url} (attempt {attempt}/{max_retries}): {e}"
                )
                node.record_failure()
                tried_urls.add(node.url)
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(
                    f"Connection/HTTP error on {node.url} (attempt {attempt}/{max_retries}): {e}"
                )
                node.record_failure()
                tried_urls.add(node.url)
            finally:
                self.release_node(node)

            sleep_time = backoff + random.uniform(0.1, 0.4)
            time.sleep(sleep_time)
            backoff *= 1.5

        raise RuntimeError(
            f"Failed to fetch logs after {max_retries} attempts across all RPC nodes. Filter: {filter_params}"
        )

    @property
    def w3(self) -> Web3:
        return self.current_node.w3

    def get_erc20_balance(self, token_address: str, wallet: str) -> int:
        checksum_token = Web3.to_checksum_address(token_address)
        checksum_wallet = Web3.to_checksum_address(wallet)
        for _ in range(len(self.nodes)):
            node = self.current_node
            try:
                contract = node.w3.eth.contract(
                    address=checksum_token, abi=ERC20_BALANCE_OF_ABI
                )
                bal = contract.functions.balanceOf(checksum_wallet).call()
                node.record_success()
                return int(bal)
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(f"Error calling balanceOf on {node.url}: {e}")
                self.rotate_node()
        raise RuntimeError(f"All RPC nodes failed calling balanceOf on {token_address}")

    def get_erc1155_balance(self, ctf_address: str, wallet: str, token_id: int) -> int:
        checksum_ctf = Web3.to_checksum_address(ctf_address)
        checksum_wallet = Web3.to_checksum_address(wallet)
        for _ in range(len(self.nodes)):
            node = self.current_node
            try:
                contract = node.w3.eth.contract(
                    address=checksum_ctf, abi=ERC1155_BALANCE_OF_ABI
                )
                bal = contract.functions.balanceOf(checksum_wallet, token_id).call()
                node.record_success()
                return int(bal)
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(f"Error calling balanceOf 1155 on {node.url}: {e}")
                self.rotate_node()
        raise RuntimeError(
            f"All RPC nodes failed calling balanceOf ERC-1155 on {ctf_address}, id {token_id}"
        )


rpc_client = MultiRpcClient()
