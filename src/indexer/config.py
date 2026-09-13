from functools import cached_property

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from web3 import Web3


class Settings(BaseSettings):
    database_url: str = "sqlite://polymarket.db"
    wallet: str = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"
    start_block: int | None = None
    polygon_rpc_urls: str = (
        "https://polygon.gateway.tenderly.co,https://gateway.tenderly.co/public/polygon"
    )
    polygon_rpc_url: str | None = None
    scanner_concurrency: int = 2
    chunk_size: int = 15_000
    max_chunk_size: int = 25_000
    normalizer_concurrency: int = 4
    normalizer_chunk_size: int = 50_000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("wallet")
    @classmethod
    def validate_wallet(cls, v: str) -> str:
        if not Web3.is_address(v):
            raise ValueError(f"Invalid Ethereum address: {v}")
        return Web3.to_checksum_address(v)

    @cached_property
    def checksum_wallet(self) -> str:
        return Web3.to_checksum_address(self.wallet)

    @cached_property
    def wallet_topic(self) -> str:
        # 32-байтный hex для фильтрации по topic
        raw = self.checksum_wallet[2:].lower()
        return "0x" + raw.rjust(64, "0")

    @property
    def rpc_urls(self) -> list[str]:
        urls = [u.strip() for u in self.polygon_rpc_urls.split(",") if u.strip()]
        if self.polygon_rpc_url and self.polygon_rpc_url.strip() not in urls:
            urls.insert(0, self.polygon_rpc_url.strip())
        return urls


settings = Settings()
