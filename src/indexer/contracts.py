from web3 import Web3

# Collateral Tokens
USDC_E = Web3.to_checksum_address(
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
)  # Bridged USDC.e
PUSD = Web3.to_checksum_address(
    "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
)  # Polymarket pUSD
COLLATERAL_TOKENS = {USDC_E, PUSD}

# Core Prediction Market Contracts
CTF = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")

# Exchanges
CTF_EXCHANGE_V1 = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
CTF_EXCHANGE_V2 = Web3.to_checksum_address("0xE111180000d2663C0091e4f400237545B87B996B")
NEG_RISK_EXCHANGE_V1 = Web3.to_checksum_address(
    "0xC5d563A36AE78145C45a50134d48A1215220f80a"
)
NEG_RISK_EXCHANGE_V2 = Web3.to_checksum_address(
    "0xe2222d279d744050d28e00520010520000310F59"
)
NEG_RISK_ADAPTER = Web3.to_checksum_address(
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
)

EXCHANGE_CONTRACTS = {
    CTF_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_EXCHANGE_V1,
    NEG_RISK_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
}

# Collateral Wrappers
COLLATERAL_ONRAMP = Web3.to_checksum_address(
    "0x93070a847efEf7F70739046A929D47a521F5B8ee"
)
COLLATERAL_OFFRAMP = Web3.to_checksum_address(
    "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"
)

ALL_RELEVANT_CONTRACTS = (
    COLLATERAL_TOKENS
    | {CTF}
    | EXCHANGE_CONTRACTS
    | {COLLATERAL_ONRAMP, COLLATERAL_OFFRAMP}
)


def to_topic(signature: str) -> str:
    """Computes canonical 0x-prefixed 32-byte keccak-256 topic hash for an event signature."""
    h = Web3.keccak(text=signature).hex()
    return h if h.startswith("0x") else f"0x{h}"


# Canonical Event Signatures & Topics
# ERC-20
TOPIC_TRANSFER_ERC20 = to_topic("Transfer(address,address,uint256)")

# ERC-1155 (CTF)
TOPIC_TRANSFER_SINGLE = to_topic(
    "TransferSingle(address,address,address,uint256,uint256)"
)
TOPIC_TRANSFER_BATCH = to_topic(
    "TransferBatch(address,address,address,uint256[],uint256[])"
)

# CTF Operations
TOPIC_POSITION_SPLIT = to_topic(
    "PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)"
)
TOPIC_POSITIONS_MERGE = to_topic(
    "PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)"
)
TOPIC_PAYOUT_REDEMPTION = to_topic(
    "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)"
)

# Exchange Trades
TOPIC_ORDER_FILLED_V1 = to_topic(
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
)
TOPIC_ORDER_FILLED_V2 = to_topic(
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
)
TOPIC_ORDERS_MATCHED_V1 = to_topic(
    "OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)"
)
TOPIC_ORDERS_MATCHED_V2 = to_topic(
    "OrdersMatched(bytes32,address,uint8,uint256,uint256,uint256)"
)

# Standard ABI for ERC-20 and ERC-1155 balanceOf
ERC20_BALANCE_OF_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    }
]

ERC1155_BALANCE_OF_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    }
]
