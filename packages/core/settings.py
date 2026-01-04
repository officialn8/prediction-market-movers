"""
Central configuration using Pydantic Settings.
Handles environment variables and .env file loading.
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # Database Configuration
    database_url: str = Field(
        default="postgresql://pmuser:pmpass@localhost:5432/prediction_movers",
        description="PostgreSQL connection string",
        validation_alias="DATABASE_URL",
    )
    db_pool_min_size: int = Field(default=2, ge=1, le=10)
    db_pool_max_size: int = Field(default=10, ge=2, le=50)
    db_connection_timeout: float = Field(default=30.0, ge=5.0)
    
    # API Keys
    polymarket_api_key: Optional[str] = Field(default=None)
    kalshi_api_key: Optional[str] = Field(default=None)
    kalshi_api_secret: Optional[str] = Field(default=None)
    
    # Collector Settings
    sync_interval_seconds: int = Field(default=300, ge=60, le=3600)
    log_level: str = Field(default="INFO")

    # Polymarket WebSocket Settings
    polymarket_use_wss: bool = Field(
        default=False,
        description="Enable WebSocket for real-time updates"
    )
    wss_reconnect_delay: float = Field(
        default=5.0,
        description="Seconds to wait before reconnecting"
    )
    wss_max_reconnect_attempts: int = Field(
        default=10,
        description="Max reconnection attempts before fallback"
    )
    wss_batch_size: int = Field(
        default=100,
        description="Batch size for DB writes"
    )
    wss_batch_interval: float = Field(
        default=1.0,
        description="Max seconds between batch flushes"
    )
    wss_fallback_to_polling: bool = Field(
        default=True,
        description="Fall back to REST polling on WSS failure"
    )
    wss_watchdog_timeout: int = Field(
        default=120,
        description="Seconds without messages before forcing reconnect"
    )
    
    # Streamlit Settings
    streamlit_server_port: int = Field(default=8501)
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper_v = v.upper()
        if upper_v not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return upper_v
    
    @property
    def db_async_url(self) -> str:
        """Convert standard postgres URL to async (asyncpg) format."""
        if self.database_url.startswith("postgresql://"):
            return self.database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return self.database_url


@lru_cache
def get_settings() -> Settings:
    """
    Cached settings instance.
    Use this function to get settings throughout the application.
    """
    return Settings()


# Convenience export
settings = get_settings()

