import argparse
import asyncio
import logging

from web3 import Web3

from .config import settings
from .contracts import PUSD, USDC_E
from .db import (
    close_db,
    get_balance_changes_stats,
    get_checkpoint,
    get_raw_logs_stats,
    init_db,
    save_checkpoint,
)
from .ledger import update_current_balances
from .normalizer import TransactionNormalizer
from .rpc import rpc_client
from .scanner import BlockchainScanner
from .verifier import OnChainVerifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("polymarket-indexer")


def resolve_wallet(wallet_arg: str | None) -> str:
    raw = wallet_arg or settings.wallet
    if not Web3.is_address(raw):
        raise ValueError(f"Invalid Ethereum address: {raw}")
    return Web3.to_checksum_address(raw)


async def show_status(wallet: str | None = None) -> None:
    await init_db()
    target_wallet = resolve_wallet(wallet)
    latest_block = rpc_client.get_latest_block()
    active_rpc = rpc_client.current_node.url
    checkpoint_id = f"wallet_{target_wallet.lower()}"
    last_block = await get_checkpoint(checkpoint_id, default=0)

    logger.info("=== Polymarket Indexer Status (Tortoise ORM) ===")
    logger.info(f"Target wallet:     {target_wallet}")
    logger.info(f"Connected RPC:     {active_rpc}")
    logger.info(f"Latest block:      {latest_block:,}")
    if last_block > 0:
        logger.info(
            f"Checkpoint block:  {last_block:,} (lag: {latest_block - last_block:,} blocks)"
        )
    else:
        logger.info("Checkpoint block:  None (not scanned yet)")

    raw_stats = await get_raw_logs_stats()
    logger.info(f"Stored raw logs:   {raw_stats['total_logs']:,}")
    if raw_stats["total_logs"] > 0:
        logger.info(
            f"Block range in DB: {raw_stats['min_block']:,} .. {raw_stats['max_block']:,}"
        )
        logger.info("Logs by event type:")
        for ev, cnt in raw_stats["by_event"].items():
            logger.info(f"  - {ev:20}: {cnt:,}")

    bc_stats = await get_balance_changes_stats()
    logger.info(f"Balance changes:   {bc_stats['total_changes']:,}")
    if bc_stats["total_changes"] > 0:
        logger.info("Changes by operation:")
        for op, cnt in bc_stats["by_operation"].items():
            logger.info(f"  - {op:20}: {cnt:,}")

    active_count = await update_current_balances(target_wallet)
    logger.info(f"Active positions:  {active_count:,} non-zero positions in database")

    pusd_raw = rpc_client.get_erc20_balance(PUSD, target_wallet)
    usdc_raw = rpc_client.get_erc20_balance(USDC_E, target_wallet)
    logger.info(f"On-chain pUSD:     {pusd_raw / 1e6:.6f} pUSD ({pusd_raw:,} raw)")
    logger.info(f"On-chain USDC.e:   {usdc_raw / 1e6:.6f} USDC.e ({usdc_raw:,} raw)")


async def run_scan(
    wallet: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    chunks: int | None = None,
    concurrency: int | None = None,
    chunk_size: int | None = None,
    auto_normalize: bool = True,
) -> None:
    await init_db()
    target_wallet = resolve_wallet(wallet)
    checkpoint_id = f"wallet_{target_wallet.lower()}"

    if from_block is not None:
        await save_checkpoint(checkpoint_id, from_block - 1)
        logger.info(f"Overrode checkpoint to block {from_block - 1}")

    scanner = BlockchainScanner(
        wallet=target_wallet,
        concurrency=concurrency,
        initial_chunk_size=chunk_size,
    )
    await scanner.run(target_block=to_block, max_chunks=chunks)

    if auto_normalize:
        logger.info("Normalizing newly scanned transactions...")
        normalizer = TransactionNormalizer(
            wallet=target_wallet,
            concurrency=concurrency,
        )
        await normalizer.process_all()
        await update_current_balances(wallet=target_wallet)


async def run_normalize(
    wallet: str | None = None,
    concurrency: int | None = None,
    chunk_size: int | None = None,
) -> None:
    await init_db()
    target_wallet = resolve_wallet(wallet)
    normalizer = TransactionNormalizer(
        wallet=target_wallet,
        concurrency=concurrency,
        chunk_size=chunk_size,
    )
    await normalizer.process_all()
    await update_current_balances(wallet=target_wallet)


async def run_verify(
    wallet: str | None = None,
    block: int | None = None,
    limit: int | None = 20,
    check_all: bool = False,
) -> None:
    await init_db()
    target_wallet = resolve_wallet(wallet)
    checkpoint_id = f"wallet_{target_wallet.lower()}"
    verify_block = block or await get_checkpoint(checkpoint_id, default=0)
    if verify_block == 0:
        verify_block = rpc_client.get_latest_block()

    logger.info(f"Running On-Chain Verification against block {verify_block:,}...")
    verifier = OnChainVerifier(wallet=target_wallet)
    report = await verifier.verify_at_block(
        block_identifier=verify_block,
        limit_positions=None if check_all else limit,
    )

    logger.info(f"=== Verification Report (Block {verify_block:,}) ===")
    logger.info(
        f"Checked: {report['checked_count']} | "
        f"Matched: {report['matched_count']} | "
        f"Mismatches: {report['mismatch_count']} | "
        f"All Matched: {report['all_matched']}"
    )

    for r in report["results"]:
        status = "OK" if r["match"] else "FAIL"
        diff_str = f"diff={r['diff']}" if r["diff"] is not None else "diff=ERR"
        logger.info(
            f"[{status}] {r['token']:22} | "
            f"Calc: {r['calc_balance']:12,} | "
            f"Actual: {r['actual_balance']:12,} | {diff_str}"
        )


async def run_live(wallet: str | None = None, poll_interval: float = 3.0) -> None:
    await init_db()
    target_wallet = resolve_wallet(wallet)
    scanner = BlockchainScanner(wallet=target_wallet)
    await scanner.run_live(poll_interval=poll_interval)


async def async_main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket On-Chain Indexer (Tortoise ORM)"
    )
    subparsers = parser.add_subparsers(dest="command")

    status_parser = subparsers.add_parser(
        "status", help="Show current indexer status and balances"
    )
    status_parser.add_argument(
        "wallet", nargs="?", default=None, help="Target wallet address"
    )

    norm_parser = subparsers.add_parser(
        "normalize", help="Normalize raw logs into balance changes and ledger"
    )
    norm_parser.add_argument(
        "wallet", nargs="?", default=None, help="Target wallet address"
    )
    norm_parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=None,
        help="Number of concurrent normalizer workers (default: 4)",
    )
    norm_parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Block range per chunk (default: 50,000)",
    )

    verify_parser = subparsers.add_parser(
        "verify", help="Verify calculated balances against on-chain RPC balanceOf"
    )
    verify_parser.add_argument(
        "wallet", nargs="?", default=None, help="Target wallet address"
    )
    verify_parser.add_argument(
        "--block",
        type=int,
        default=None,
        help="Block number to verify against (default: checkpoint)",
    )
    verify_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of positions to check (default: 20)",
    )
    verify_parser.add_argument("--all", action="store_true", help="Check all positions")

    scan_parser = subparsers.add_parser(
        "scan", help="Run historical on-chain event backfill"
    )
    scan_parser.add_argument(
        "wallet", nargs="?", default=None, help="Target wallet address"
    )
    scan_parser.add_argument(
        "--from-block", type=int, default=None, help="Starting block number"
    )
    scan_parser.add_argument(
        "--to-block", type=int, default=None, help="Target block number"
    )
    scan_parser.add_argument(
        "--chunks", type=int, default=None, help="Limit number of chunks to process"
    )
    scan_parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=None,
        help="Number of concurrent chunk workers (default: 4)",
    )
    scan_parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Block range per chunk (default: 25,000)",
    )
    scan_parser.add_argument(
        "--no-normalize", action="store_true", help="Do not run normalizer after scan"
    )

    live_parser = subparsers.add_parser(
        "live", help="Continuously index new blocks as they are produced"
    )
    live_parser.add_argument(
        "wallet", nargs="?", default=None, help="Target wallet address"
    )
    live_parser.add_argument(
        "--interval", type=float, default=3.0, help="Poll interval in seconds"
    )

    args = parser.parse_args()

    if hasattr(args, "wallet") and args.wallet and not Web3.is_address(args.wallet):
        parser.error(f"Invalid Ethereum wallet address: {args.wallet}")

    try:
        if args.command == "status":
            await show_status(wallet=args.wallet)
        elif args.command == "normalize":
            await run_normalize(
                wallet=args.wallet,
                concurrency=args.concurrency,
                chunk_size=args.chunk_size,
            )
        elif args.command == "verify":
            await run_verify(
                wallet=args.wallet,
                block=args.block,
                limit=args.limit,
                check_all=args.all,
            )
        elif args.command == "scan":
            await run_scan(
                wallet=args.wallet,
                from_block=args.from_block,
                to_block=args.to_block,
                chunks=args.chunks,
                concurrency=args.concurrency,
                chunk_size=args.chunk_size,
                auto_normalize=not args.no_normalize,
            )
        elif args.command == "live":
            await run_live(wallet=args.wallet, poll_interval=args.interval)
        else:
            await show_status()
    finally:
        await close_db()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
