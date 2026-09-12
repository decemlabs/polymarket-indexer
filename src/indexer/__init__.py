import decimal

# Точность Decimal для 256-битных целых чисел (до 78 знаков)
decimal.getcontext().prec = 100
