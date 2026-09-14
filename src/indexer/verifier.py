import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any

from web3 import Web3
from web3.exceptions import Web3Exception
from web3.types import BlockIdentifier

from .config import settings
from .contracts import CTF, ERC20_BALANCE_OF_ABI, ERC1155_BALANCE_OF_ABI, PUSD, USDC_E
from .ledger import get_current_balances, update_current_balances
from .models import CurrentBalance
from .rpc import MultiRpcClient, rpc_client

logger = logging.getLogger(__name__)


def to_int_balance(val: int | str | Decimal) -> int:
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if "." in s:
        return int(Decimal(s))
    return int(s)


def to_int_token_id(val: int | str | Decimal) -> int:
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if "e" in s or "E" in s or "." in s:
        return int(Decimal(s))
    return int(s)


class OnChainVerifier:
    rpc: MultiRpcClient
    wallet: str

    def __init__(
        self,
        rpc: MultiRpcClient | None = None,
        wallet: str | None = None,
    ):
        self.rpc = rpc or rpc_client
        self.wallet = Web3.to_checksum_address(wallet or settings.checksum_wallet)

    async def verify_at_block(
        self,
        block_identifier: BlockIdentifier = "latest",
        limit_positions: int | None = None,
        force_replay: bool = False,
    ) -> dict[str, Any]:
        existing_count = await CurrentBalance.filter(wallet=self.wallet).count()
        if existing_count == 0 or force_replay:
            await update_current_balances(self.wallet)
        calculated_positions = await get_current_balances(self.wallet, include_zero=False)

        is_historical = isinstance(block_identifier, int) or (
            isinstance(block_identifier, str) and block_identifier.isdigit()
        )

        erc20_node = self.rpc.acquire_node(require_archive=is_historical)
        try:
            w3 = erc20_node.w3
            usdc_contract = w3.eth.contract(address=USDC_E, abi=ERC20_BALANCE_OF_ABI)
            pusd_contract = w3.eth.contract(address=PUSD, abi=ERC20_BALANCE_OF_ABI)

            results: list[dict[str, Any]] = []
            checked_count = 0
            matched_count = 0
            mismatch_count = 0
            negative_count = 0
            error_count = 0

            # 1. ERC-20 collateral check (USDC.e and pUSD)
            calc_usdc = 0
            calc_pusd = 0
            for pos in calculated_positions:
                if pos["token_type"] == "ERC20":
                    if pos["token_address"].lower() == USDC_E.lower():
                        calc_usdc = to_int_balance(pos["balance"])
                    elif pos["token_address"].lower() == PUSD.lower():
                        calc_pusd = to_int_balance(pos["balance"])

            # Check USDC.e
            try:
                actual_usdc = await asyncio.to_thread(
                    usdc_contract.functions.balanceOf(self.wallet).call,
                    block_identifier=block_identifier,
                )
                diff_usdc = calc_usdc - actual_usdc
                match_usdc = diff_usdc == 0
                is_neg_usdc = calc_usdc < 0
                checked_count += 1
                if is_neg_usdc:
                    negative_count += 1
                if match_usdc:
                    matched_count += 1
                else:
                    mismatch_count += 1

                results.append(
                    {
                        "token": "USDC.e",
                        "token_id": 0,
                        "calc_balance": calc_usdc,
                        "actual_balance": actual_usdc,
                        "diff": diff_usdc,
                        "match": match_usdc,
                        "is_negative": is_neg_usdc,
                    }
                )
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(
                    f"Failed to check on-chain USDC.e at block {block_identifier!r}: {e}"
                )
                error_count += 1
                checked_count += 1
                results.append(
                    {
                        "token": "USDC.e",
                        "token_id": 0,
                        "calc_balance": calc_usdc,
                        "actual_balance": -1,
                        "diff": None,
                        "match": False,
                        "is_negative": calc_usdc < 0,
                        "error": str(e),
                    }
                )

            # Check pUSD
            try:
                actual_pusd = await asyncio.to_thread(
                    pusd_contract.functions.balanceOf(self.wallet).call,
                    block_identifier=block_identifier,
                )
                diff_pusd = calc_pusd - actual_pusd
                match_pusd = diff_pusd == 0
                is_neg_pusd = calc_pusd < 0
                checked_count += 1
                if is_neg_pusd:
                    negative_count += 1
                if match_pusd:
                    matched_count += 1
                else:
                    mismatch_count += 1

                results.append(
                    {
                        "token": "pUSD",
                        "token_id": 0,
                        "calc_balance": calc_pusd,
                        "actual_balance": actual_pusd,
                        "diff": diff_pusd,
                        "match": match_pusd,
                        "is_negative": is_neg_pusd,
                    }
                )
            except (Web3Exception, OSError, ValueError) as e:
                logger.warning(
                    f"Failed to check on-chain pUSD at block {block_identifier!r}: {e}"
                )
                error_count += 1
                checked_count += 1
                results.append(
                    {
                        "token": "pUSD",
                        "token_id": 0,
                        "calc_balance": calc_pusd,
                        "actual_balance": -1,
                        "diff": None,
                        "match": False,
                        "is_negative": calc_pusd < 0,
                        "error": str(e),
                    }
                )
        finally:
            self.rpc.release_node(erc20_node)

        # 2. ERC-1155 outcome tokens (CTF)
        erc1155_positions = [
            pos for pos in calculated_positions if pos["token_type"] == "ERC1155"
        ]
        total_erc1155 = len(erc1155_positions)
        is_partial = limit_positions is not None and limit_positions < total_erc1155

        to_check = (
            erc1155_positions[:limit_positions] if is_partial else erc1155_positions
        )

        batch_size = 500
        chunks = [
            to_check[i : i + batch_size]
            for i in range(0, len(to_check), batch_size)
        ]

        def check_chunk(chunk: list[dict[str, Any]]) -> list[dict[str, Any]]:
            tids = [to_int_token_id(p["token_id"]) for p in chunk]
            owners = [self.wallet] * len(tids)
            chunk_results: list[dict[str, Any]] = []

            attempt_exclude_urls: set[str] = set()
            for attempt in range(5):
                node = self.rpc.acquire_node(
                    exclude_urls=attempt_exclude_urls,
                    require_archive=is_historical,
                )
                attempt_exclude_urls.add(node.url)
                try:
                    ctf = node.w3.eth.contract(address=CTF, abi=ERC1155_BALANCE_OF_ABI)
                    actuals = ctf.functions.balanceOfBatch(
                        owners, tids
                    ).call(block_identifier=block_identifier)
                    node.record_success()

                    for pos, tid, actual in zip(chunk, tids, actuals):
                        calc = to_int_balance(pos["balance"])
                        diff = calc - actual
                        is_neg = calc < 0
                        chunk_results.append(
                            {
                                "token": f"CTF {str(tid)[:14]}...",
                                "token_id": tid,
                                "calc_balance": calc,
                                "actual_balance": int(actual),
                                "diff": diff,
                                "match": (diff == 0),
                                "is_negative": is_neg,
                            }
                        )
                    return chunk_results
                except (Web3Exception, OSError, ValueError) as batch_err:
                    err_str = str(batch_err)
                    is_arch_err = "historical state" in err_str or "-32000" in err_str
                    node.record_failure(is_archive_error=is_arch_err)
                    if attempt == 4:
                        logger.warning(
                            f"balanceOfBatch failed after 5 attempts ({batch_err}). "
                            "Falling back to individual queries."
                        )
                finally:
                    self.rpc.release_node(node)

            for pos, tid in zip(chunk, tids):
                calc = to_int_balance(pos["balance"])
                is_neg = calc < 0
                node = self.rpc.acquire_node(require_archive=is_historical)
                try:
                    ctf = node.w3.eth.contract(address=CTF, abi=ERC1155_BALANCE_OF_ABI)
                    actual = ctf.functions.balanceOf(
                        self.wallet, tid
                    ).call(block_identifier=block_identifier)
                    node.record_success()
                    diff = calc - actual
                    chunk_results.append(
                        {
                            "token": f"CTF {str(tid)[:14]}...",
                            "token_id": tid,
                            "calc_balance": calc,
                            "actual_balance": int(actual),
                            "diff": diff,
                            "match": (diff == 0),
                            "is_negative": is_neg,
                        }
                    )
                except (Web3Exception, OSError, ValueError) as single_err:
                    err_str = str(single_err)
                    is_arch_err = "historical state" in err_str or "-32000" in err_str
                    node.record_failure(is_archive_error=is_arch_err)
                    chunk_results.append(
                        {
                            "token": f"CTF {str(tid)[:14]}...",
                            "token_id": tid,
                            "calc_balance": calc,
                            "actual_balance": -1,
                            "diff": None,
                            "match": False,
                            "is_negative": is_neg,
                            "error": str(single_err),
                        }
                    )
                finally:
                    self.rpc.release_node(node)
            return chunk_results

        if chunks:
            def run_all_chunks():
                with ThreadPoolExecutor(max_workers=20) as executor:
                    chunk_res_lists = list(executor.map(check_chunk, chunks))
                flat = []
                for cl in chunk_res_lists:
                    flat.extend(cl)
                return flat

            erc1155_results = await asyncio.to_thread(run_all_chunks)
            for r in erc1155_results:
                checked_count += 1
                if r.get("is_negative"):
                    negative_count += 1
                if r.get("error"):
                    error_count += 1
                    mismatch_count += 1
                elif r["match"]:
                    matched_count += 1
                else:
                    mismatch_count += 1
                results.append(r)

        total_wallet_positions = total_erc1155 + (1 if calc_usdc != 0 else 0) + (1 if calc_pusd != 0 else 0)

        # Status & all_matched calculation
        all_matched = False
        if negative_count > 0:
            status = f"FAILED: {negative_count} position(s) have NEGATIVE local balance (ledger invariant violated)"
        elif mismatch_count > 0:
            status = f"FAILED: {mismatch_count} position(s) mismatch on-chain"
        elif error_count > 0:
            status = f"FAILED: {error_count} position(s) could not be checked due to RPC errors"
        elif is_partial:
            status = f"PARTIAL_SAMPLE: {checked_count}/{total_wallet_positions} positions checked (sample matched, but full wallet NOT verified)"
        else:
            status = f"SUCCESS: All {checked_count} positions in wallet verified and match on-chain"
            all_matched = True

        return {
            "wallet": self.wallet,
            "block_identifier": block_identifier,
            "total_wallet_positions": total_wallet_positions,
            "checked_count": checked_count,
            "matched_count": matched_count,
            "mismatch_count": mismatch_count,
            "negative_count": negative_count,
            "error_count": error_count,
            "is_partial": is_partial,
            "all_matched": all_matched,
            "status": status,
            "results": results,
        }

