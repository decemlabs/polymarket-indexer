import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any

from web3 import Web3

from .config import settings
from .contracts import CTF, ERC20_BALANCE_OF_ABI, ERC1155_BALANCE_OF_ABI, PUSD, USDC_E
from .ledger import get_current_balances, update_current_balances
from .rpc import MultiRpcClient, rpc_client

logger = logging.getLogger(__name__)


def to_int_balance(val: Any) -> int:
    """Safely converts string, decimal, or float representation to exact integer."""
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if "." in s:
        return int(Decimal(s))
    return int(s)


def to_int_token_id(val: Any) -> int:
    """Safely converts token ID (including legacy scientific strings) to exact integer."""
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if "e" in s or "E" in s or "." in s:
        return int(Decimal(s))
    return int(s)


class OnChainVerifier:
    def __init__(
        self,
        rpc: MultiRpcClient | None = None,
        wallet: str | None = None,
    ):
        self.rpc = rpc or rpc_client
        self.wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)

    async def verify_at_block(
        self,
        block_identifier: int | str,
        limit_positions: int | None = None,
    ) -> dict[str, Any]:
        """
        Compares calculated balances against on-chain balanceOf calls at the specified block.
        """
        # Ensure current_balances is up to date
        await update_current_balances(self.wallet)
        calculated_positions = await get_current_balances(self.wallet)

        w3 = self.rpc.current_node.w3
        ctf_contract = w3.eth.contract(address=CTF, abi=ERC1155_BALANCE_OF_ABI)
        usdc_contract = w3.eth.contract(address=USDC_E, abi=ERC20_BALANCE_OF_ABI)
        pusd_contract = w3.eth.contract(address=PUSD, abi=ERC20_BALANCE_OF_ABI)

        report: dict[str, Any] = {
            "wallet": self.wallet,
            "block_identifier": block_identifier,
            "checked_count": 0,
            "matched_count": 0,
            "mismatch_count": 0,
            "results": [],
            "all_matched": True,
        }

        # 1. Verify ERC-20 collateral tokens
        calc_usdc = 0
        calc_pusd = 0
        for pos in calculated_positions:
            if pos["token_type"] == "ERC20":
                if pos["token_address"].lower() == USDC_E.lower():
                    calc_usdc = to_int_balance(pos["balance"])
                elif pos["token_address"].lower() == PUSD.lower():
                    calc_pusd = to_int_balance(pos["balance"])

        try:
            actual_usdc = await asyncio.to_thread(
                usdc_contract.functions.balanceOf(self.wallet).call,
                block_identifier=block_identifier,
            )
            diff_usdc = calc_usdc - actual_usdc
            match_usdc = diff_usdc == 0
            report["checked_count"] += 1
            if match_usdc:
                report["matched_count"] += 1
            else:
                report["mismatch_count"] += 1
                report["all_matched"] = False

            report["results"].append(
                {
                    "token": "USDC.e",
                    "token_id": 0,
                    "calc_balance": calc_usdc,
                    "actual_balance": actual_usdc,
                    "diff": diff_usdc,
                    "match": match_usdc,
                }
            )
        except Exception as e:
            logger.warning(
                f"Failed to check on-chain USDC.e at block {block_identifier}: {e}"
            )

        # Check pUSD
        try:
            actual_pusd = await asyncio.to_thread(
                pusd_contract.functions.balanceOf(self.wallet).call,
                block_identifier=block_identifier,
            )
            diff_pusd = calc_pusd - actual_pusd
            match_pusd = diff_pusd == 0
            report["checked_count"] += 1
            if match_pusd:
                report["matched_count"] += 1
            else:
                report["mismatch_count"] += 1
                report["all_matched"] = False

            report["results"].append(
                {
                    "token": "pUSD",
                    "token_id": 0,
                    "calc_balance": calc_pusd,
                    "actual_balance": actual_pusd,
                    "diff": diff_pusd,
                    "match": match_pusd,
                }
            )
        except Exception as e:
            logger.warning(
                f"Failed to check on-chain pUSD at block {block_identifier}: {e}"
            )

        # 2. Verify ERC-1155 positions
        erc1155_positions = [
            p for p in calculated_positions if p["token_type"] == "ERC1155"
        ]
        if limit_positions is not None:
            erc1155_positions = erc1155_positions[:limit_positions]

        def check_token(pos: dict[str, Any]) -> dict[str, Any]:
            tid = to_int_token_id(pos["token_id"])
            calc = to_int_balance(pos["balance"])
            try:
                actual = ctf_contract.functions.balanceOf(self.wallet, tid).call(
                    block_identifier=block_identifier
                )
                diff = calc - actual
                return {
                    "token": f"CTF {str(tid)[:14]}...",
                    "token_id": tid,
                    "calc_balance": calc,
                    "actual_balance": actual,
                    "diff": diff,
                    "match": (diff == 0),
                }
            except Exception as e:
                return {
                    "token": f"CTF {str(tid)[:14]}...",
                    "token_id": tid,
                    "calc_balance": calc,
                    "actual_balance": -1,
                    "diff": None,
                    "match": False,
                    "error": str(e),
                }

        def run_thread_checks():
            with ThreadPoolExecutor(max_workers=5) as executor:
                return list(executor.map(check_token, erc1155_positions))

        token_results = await asyncio.to_thread(run_thread_checks)

        for res in token_results:
            report["checked_count"] += 1
            if res["match"]:
                report["matched_count"] += 1
            else:
                report["mismatch_count"] += 1
                report["all_matched"] = False
            report["results"].append(res)

        return report
