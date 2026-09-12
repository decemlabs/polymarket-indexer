import decimal

# Set Decimal precision to 100 digits to accommodate uint256 integers (up to 78 digits)
decimal.getcontext().prec = 100
