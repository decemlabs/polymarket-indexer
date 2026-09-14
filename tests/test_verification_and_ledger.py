import asyncio
import os
import tempfile
from decimal import Decimal
from unittest.mock import MagicMock

from tortoise import Tortoise
from web3 import Web3

from indexer.contracts import CTF, PUSD, USDC_E
from indexer.db import init_db, close_db
from indexer.ledger import get_current_balances, update_current_balances
from indexer.models import BalanceChange, CurrentBalance
from indexer.verifier import OnChainVerifier


async def run_tests():
    # 1. Initialize temporary SQLite database
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = tmp.name

    test_db_url = f"sqlite://{db_path}"
    await Tortoise.init(
        db_url=test_db_url,
        modules={"models": ["indexer.models"]},
    )
    await Tortoise.generate_schemas()

    test_wallet = "0x1111111111111111111111111111111111111111"

    print("Test 1: Check that negative balances are NOT deleted from current_balances...")
    # Add one positive balance, one zero balance, and one NEGATIVE balance
    # Positive: token_id "100", amount +50
    await BalanceChange.create(
        wallet=test_wallet,
        block_number=1000,
        transaction_hash="0x01",
        log_index=0,
        operation_type="TRADE_BUY",
        token_type="ERC1155",
        token_address=CTF,
        token_id="100",
        amount_delta=Decimal("50"),
    )
    # Zero: token_id "200", amount +10 then -10
    await BalanceChange.create(
        wallet=test_wallet,
        block_number=1001,
        transaction_hash="0x02",
        log_index=0,
        operation_type="TRADE_BUY",
        token_type="ERC1155",
        token_address=CTF,
        token_id="200",
        amount_delta=Decimal("10"),
    )
    await BalanceChange.create(
        wallet=test_wallet,
        block_number=1002,
        transaction_hash="0x03",
        log_index=0,
        operation_type="TRADE_SELL",
        token_type="ERC1155",
        token_address=CTF,
        token_id="200",
        amount_delta=Decimal("-10"),
    )
    # NEGATIVE: token_id "300", amount -25 (e.g. missed mint/deposit)
    await BalanceChange.create(
        wallet=test_wallet,
        block_number=1003,
        transaction_hash="0x04",
        log_index=0,
        operation_type="TRADE_SELL",
        token_type="ERC1155",
        token_address=CTF,
        token_id="300",
        amount_delta=Decimal("-25"),
    )

    await update_current_balances(test_wallet)

    # Check database contents
    all_rows = await CurrentBalance.filter(wallet=test_wallet).all()
    token_map = {r.token_id: Decimal(str(r.balance)) for r in all_rows}

    assert "100" in token_map, "Positive balance token 100 should exist"
    assert token_map["100"] == Decimal("50"), f"Expected 50, got {token_map['100']}"

    assert "200" not in token_map, "Zero balance token 200 should have been deleted"

    assert "300" in token_map, "CRITICAL: Negative balance token 300 MUST NOT be deleted!"
    assert token_map["300"] == Decimal("-25"), f"Expected -25, got {token_map['300']}"

    print(" -> PASSED: Negative balance preserved, zero balance deleted.")

    print("Test 2: Check get_current_balances returns negative balances sorted to the top...")
    positions = await get_current_balances(test_wallet)
    assert len(positions) == 2, f"Expected 2 positions, got {len(positions)}"
    assert positions[0]["token_id"] == "300", "Negative balance position should be sorted to the very top"
    assert Decimal(str(positions[0]["balance"])) == Decimal("-25")
    assert positions[1]["token_id"] == "100"
    print(" -> PASSED: get_current_balances includes negative balances and sorts them to the top.")

    print("Test 3: Check OnChainVerifier flags negative balances and fails verification...")
    # Mock RPC and contracts
    mock_rpc = MagicMock()
    mock_node = MagicMock()
    mock_rpc.current_node = mock_node
    mock_rpc.acquire_node.return_value = mock_node
    mock_w3 = MagicMock()
    mock_node.w3 = mock_w3

    mock_ctf = MagicMock()
    mock_usdc = MagicMock()
    mock_pusd = MagicMock()

    def mock_contract(address, abi):
        if address == CTF:
            return mock_ctf
        elif address == USDC_E:
            return mock_usdc
        elif address == PUSD:
            return mock_pusd
        return MagicMock()

    mock_w3.eth.contract = mock_contract
    mock_usdc.functions.balanceOf.return_value.call.return_value = 0
    mock_pusd.functions.balanceOf.return_value.call.return_value = 0

    # For CTF balanceOfBatch:
    # token 300 has on-chain balance 0, token 100 has on-chain balance 50
    mock_ctf.functions.balanceOfBatch.return_value.call.return_value = [0, 50]

    verifier = OnChainVerifier(rpc=mock_rpc, wallet=test_wallet)
    report = await verifier.verify_at_block()

    assert report["negative_count"] == 1, f"Expected 1 negative balance, got {report['negative_count']}"
    assert report["mismatch_count"] == 1, f"Negative balance must count as mismatch, got {report['mismatch_count']}"
    assert report["all_matched"] is False, "all_matched MUST be False when negative balance exists!"
    assert "NEGATIVE" in report["status"], f"Status should report negative balance, got: {report['status']}"
    print(" -> PASSED: Verifier detected negative balance, failed verification, and logged status.")

    print("Test 4: Check partial verification (--limit) NEVER reports all_matched = True...")
    # Remove negative balance from both balance_changes and current_balances
    await BalanceChange.filter(wallet=test_wallet, token_id="300").delete()
    await CurrentBalance.filter(wallet=test_wallet, token_id="300").delete()
    for i in range(101, 106):
        await BalanceChange.create(
            wallet=test_wallet,
            block_number=1010 + i,
            transaction_hash=f"0x10{i}",
            log_index=0,
            operation_type="TRADE_BUY",
            token_type="ERC1155",
            token_address=CTF,
            token_id=str(i),
            amount_delta=Decimal("10"),
        )

    # Update current balances to reflect all 6 tokens
    await update_current_balances(test_wallet)

    # Now we have 6 ERC-1155 positions: 100, 101, 102, 103, 104, 105
    # Mock balanceOfBatch to return [50, 10] for the 2 positions checked in partial check
    mock_ctf.functions.balanceOfBatch.return_value.call.return_value = [50, 10]

    # Verify only top 2 positions
    partial_report = await verifier.verify_at_block(limit_positions=2)
    assert partial_report["is_partial"] is True, "is_partial should be True"
    assert partial_report["all_matched"] is False, "all_matched MUST NOT be True on partial check!"
    assert "PARTIAL_SAMPLE" in partial_report["status"], f"Status should be PARTIAL_SAMPLE, got: {partial_report['status']}"
    print(" -> PASSED: Partial check sets all_matched = False and status = PARTIAL_SAMPLE.")

    print("Test 5: Full check when all match reports all_matched = True...")
    mock_ctf.functions.balanceOfBatch.return_value.call.return_value = [50, 10, 10, 10, 10, 10]
    full_report = await verifier.verify_at_block(limit_positions=None)
    assert full_report["is_partial"] is False, "is_partial should be False on full check"
    assert full_report["negative_count"] == 0
    assert full_report["mismatch_count"] == 0
    assert full_report["all_matched"] is True, "Full check matching on-chain MUST report all_matched = True"
    assert "SUCCESS" in full_report["status"]
    print(" -> PASSED: Full check successfully reports all_matched = True.")

    await Tortoise.close_connections()
    if os.path.exists(db_path):
        os.remove(db_path)
    print("\nALL 5 TESTS PASSED SUCCESSFULLY!")


def test_verification_and_ledger():
    asyncio.run(run_tests())


if __name__ == "__main__":
    asyncio.run(run_tests())

