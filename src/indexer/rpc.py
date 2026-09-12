import logging
import random
import time
from typing import Any

from web3 import Web3
from web3.exceptions import Web3RPCError
from web3.middleware import ExtraDataToPOAMiddleware

from .config import settings
from .contracts import ERC20_BALANCE_OF_ABI, ERC1155_BALANCE_OF_ABI

logger = logging.getLogger(__name__)


class RangeLimitError(Exception):
    """Raised when an RPC node rejects a getLogs query because the block range is too large."""


class RpcNode:
    def __init__(self, url: str):
        self.url = url
        self.w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 15}))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.failure_count = 0
        self.last_failed_at = 0.0

    def is_healthy(self, cooloff_seconds: float = 30.0) -> bool:
        if self.failure_count == 0:
            return True
        # If cooled off, give it another chance
        return time.time() - self.last_failed_at > cooloff_seconds

    def record_success(self) -> None:
        self.failure_count = 0

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failed_at = time.time()


class MultiRpcClient:
    def __init__(self, urls: list[str] | None = None):
        if not urls:
            urls = settings.rpc_urls
        self.nodes = [RpcNode(url) for url in urls]
        self._current_index = 0

    @property
    def current_node(self) -> RpcNode:
        # Find next healthy node
        for i in range(len(self.nodes)):
            idx = (self._current_index + i) % len(self.nodes)
            if self.nodes[idx].is_healthy():
                self._current_index = idx
                return self.nodes[idx]
        # If all nodes are in cooloff, fallback to current
        return self.nodes[self._current_index]

    def rotate_node(self) -> RpcNode:
        old_url = self.current_node.url
        self.current_node.record_failure()
        self._current_index = (self._current_index + 1) % len(self.nodes)
        new_node = self.current_node
        logger.warning(f"Rotated RPC node from {old_url} -> {new_node.url}")
        return new_node

    def get_latest_block(self) -> int:
        for _ in range(len(self.nodes)):
            node = self.current_node
            try:
                block_number = node.w3.eth.block_number
                node.record_success()
                return int(block_number)
            except Exception as e:
                logger.warning(f"Failed to get block number from {node.url}: {e}")
                self.rotate_node()
        raise RuntimeError("All RPC nodes failed to fetch latest block number")

    def get_logs(
        self,
        filter_params: dict[str, Any],
        max_retries: int = 5,
        initial_backoff: float = 0.5,
    ) -> list[dict[str, Any]]:
        backoff = initial_backoff

        for attempt in range(1, max_retries + 1):
            node = self.current_node
            try:
                raw_logs = node.w3.eth.get_logs(filter_params)
                node.record_success()
                return [dict(log) for log in raw_logs]
            except Web3RPCError as e:
                err_msg = str(e).lower()
                # Check for block range limits
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

                # Rate limiting or server errors
                logger.warning(
                    f"RPC error on {node.url} (attempt {attempt}/{max_retries}): {e}"
                )
                self.rotate_node()
            except Exception as e:
                logger.warning(
                    f"Connection/HTTP error on {node.url} (attempt {attempt}/{max_retries}): {e}"
                )
                self.rotate_node()

            sleep_time = backoff + random.uniform(0.1, 0.5)
            time.sleep(sleep_time)
            backoff *= 2

        raise RuntimeError(
            f"Failed to fetch logs after {max_retries} attempts across all RPC nodes. Filter: {filter_params}"
        )

    @property
    def w3(self) -> Web3:
        """Returns the Web3 instance of the currently active healthy RPC node."""
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
            except Exception as e:
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
            except Exception as e:
                logger.warning(f"Error calling balanceOf 1155 on {node.url}: {e}")
                self.rotate_node()
        raise RuntimeError(
            f"All RPC nodes failed calling balanceOf ERC-1155 on {ctf_address}, id {token_id}"
        )


# Singleton RPC client instance
rpc_client = MultiRpcClient()
