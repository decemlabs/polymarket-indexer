import asyncio
import time
from web3 import Web3
from indexer.db import init_db, close_db
from indexer.verifier import OnChainVerifier
from indexer.rpc import rpc_client

TEST_WALLETS = [
    # Main indexed wallet
    "0x46B353667FD7d846AF3BbEdA6584B0E5B883d3De",
    # 12 real Polymarket user / trader addresses on Polygon
    "0x5b0c04757F6793A1Dc496FF655460DA1b9d2Dda5",
    "0x1e6aAFE6e763C5cB553785E61F262F7FCAe4aAC6",
    "0x9dDd279e33Fc18Af8e293AC5cC690E1F4880c215",
    "0xb132C327501043F348Ef2168A9fE88C98FA3cbF0",
    "0x3a3fBb9d4268b8AA78fdDf1B4A6312b8F2E74019",
    "0x7EbF2f8E9F9B029d37D761C0522B57389072EbAf",
    "0x14B2b6B4C81C99b3d2C03486954EBc1b31307773",
    "0xE1EC6FFCE64C99DFbf83a981F22c1f58c1490a84",
    "0x0e756Db30660B1094cA44cBC04eA7a0fD019Bd40",
    "0xd79cbc8AA0CE18bf37543Db0320343a5d040D0fE",
    "0x716aDEc561c245C991fAB730ab1e6b17aCf0ad76",
    "0x4F91E24BB9301762aa1F436919F601dd5FEa3C61",
]

async def run_live_tests():
    await init_db()
    print("=" * 80)
    print(f"RUNNING LIVE ON-CHAIN VERIFICATION BENCHMARK ON {len(TEST_WALLETS)} WALLETS")
    print("=" * 80)
    
    t_start = time.time()
    results_summary = []
    
    for i, wallet in enumerate(TEST_WALLETS, 1):
        t0 = time.time()
        # Test full check (or sample if wallet has 390k positions for fast run)
        verifier = OnChainVerifier(wallet=wallet)
        
        # Check wallet positions in DB first
        from indexer.models import CurrentBalance
        pos_count = await CurrentBalance.filter(wallet=Web3.to_checksum_address(wallet)).count()
        
        limit = 50 if pos_count > 1000 else None
        report = await verifier.verify_at_block(
            block_identifier="latest",
            limit_positions=limit,
        )
        elapsed = time.time() - t0
        
        status_info = {
            "index": i,
            "wallet": wallet[:10] + "..." + wallet[-6:],
            "db_positions": report["total_wallet_positions"],
            "checked": report["checked_count"],
            "matched": report["matched_count"],
            "mismatch": report["mismatch_count"],
            "negative": report["negative_count"],
            "is_partial": report["is_partial"],
            "all_matched": report["all_matched"],
            "status": report["status"],
            "time_sec": round(elapsed, 2),
        }
        results_summary.append(status_info)
        
        flag = "✅" if report["all_matched"] else ("⚠️ " if report["is_partial"] else "❌")
        print(f"[{i:2d}/{len(TEST_WALLETS)}] {flag} {wallet[:10]}... | "
              f"DB Pos: {report['total_wallet_positions']:6d} | "
              f"Checked: {report['checked_count']:4d} | "
              f"Matched: {report['matched_count']:4d} | "
              f"Mismatch: {report['mismatch_count']:4d} | "
              f"Neg: {report['negative_count']:2d} | "
              f"Status: {report['status'][:20]:20} | "
              f"AllMatched={report['all_matched']} ({elapsed:.2f}s)")
    
    total_elapsed = time.time() - t_start
    print("=" * 80)
    print(f"BENCHMARK COMPLETE: {len(TEST_WALLETS)} wallets verified in {total_elapsed:.2f}s "
          f"(avg {total_elapsed/len(TEST_WALLETS):.2f}s/wallet)")
    print("=" * 80)
    
    await close_db()

if __name__ == "__main__":
    asyncio.run(run_live_tests())
