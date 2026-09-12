from decimal import Decimal
from typing import Any

from tortoise import fields, models


class TokenIdField(fields.CharField):
    """Хранит 256-битные EVM token ID как строку без научной нотации."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 78)
        kwargs.setdefault("default", "0")
        super().__init__(**kwargs)

    def to_python_value(self, value: Any) -> str:
        if value is None:
            return "0"
        s = str(value).strip()
        if not s or s == "None":
            return "0"
        if "e" in s or "E" in s:
            return str(int(Decimal(s)))
        if "." in s:
            return str(int(Decimal(s)))
        return s

    def to_db_value(self, value: Any, instance: Any) -> str:
        return self.to_python_value(value)


class ExactDecimalField(fields.DecimalField):
    """DecimalField без .normalize() — защита от научной нотации (2E+5)."""

    def to_python_value(self, value: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value.quantize(self.quant)
        s = str(value).strip()
        return Decimal(s).quantize(self.quant)

    def to_db_value(self, value: Any, instance: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        return f"{value.quantize(self.quant):f}"


class Checkpoint(models.Model):
    id = fields.CharField(max_length=64, primary_key=True)
    last_scanned_block = fields.BigIntField()
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta(models.Model.Meta):
        table = "checkpoints"


class RawLog(models.Model):
    id = fields.BigIntField(primary_key=True)
    block_number = fields.BigIntField(db_index=True)
    block_hash = fields.CharField(max_length=66)
    transaction_hash = fields.CharField(max_length=66, db_index=True)
    transaction_index = fields.IntField()
    log_index = fields.IntField()
    contract_address = fields.CharField(max_length=42, db_index=True)
    event_name = fields.CharField(max_length=64, null=True)
    topic0 = fields.CharField(max_length=66, db_index=True)
    topic1 = fields.CharField(max_length=66, null=True)
    topic2 = fields.CharField(max_length=66, null=True)
    topic3 = fields.CharField(max_length=66, null=True)
    data = fields.TextField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta(models.Model.Meta):
        table = "raw_logs"
        unique_together = (("transaction_hash", "log_index"),)


class BalanceChange(models.Model):
    id = fields.BigIntField(primary_key=True)
    wallet = fields.CharField(max_length=42, null=True, db_index=True)
    block_number = fields.BigIntField(db_index=True)
    transaction_hash = fields.CharField(max_length=66, db_index=True)
    log_index = fields.IntField()
    operation_type = fields.CharField(max_length=32)
    token_type = fields.CharField(max_length=16)
    token_address = fields.CharField(max_length=42, db_index=True)
    token_id = TokenIdField(db_index=True)
    amount_delta = ExactDecimalField(max_digits=78, decimal_places=0)
    counterparty = fields.CharField(max_length=42, null=True)
    details: fields.JSONField = fields.JSONField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta(models.Model.Meta):
        table = "balance_changes"
        unique_together = (
            (
                "transaction_hash",
                "log_index",
                "token_address",
                "token_id",
                "amount_delta",
            ),
        )


class CurrentBalance(models.Model):
    id = fields.BigIntField(primary_key=True)
    wallet = fields.CharField(max_length=42, db_index=True)
    token_type = fields.CharField(max_length=16)
    token_address = fields.CharField(max_length=42)
    token_id = TokenIdField()
    balance = ExactDecimalField(max_digits=78, decimal_places=0, default=Decimal(0))
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta(models.Model.Meta):
        table = "current_balances"
        unique_together = (("wallet", "token_address", "token_id"),)
