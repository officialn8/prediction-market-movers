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
    kalshi_api_key: Optional[str] = Field(
        default=None,
        description="Kalshi API key ID (for WebSocket auth)"
    )
    kalshi_api_secret: Optional[str] = Field(default=None)
    kalshi_private_key_path: Optional[str] = Field(
        default=None,
        description="Path to Kalshi RSA private key PEM file (for WebSocket auth)"
    )
    kalshi_private_key: Optional[str] = Field(
        default=None,
        description="Kalshi RSA private key as PEM string (alternative to path)"
    )
    
    # Collector Settings
    sync_interval_seconds: int = Field(default=300, ge=60, le=3600)
    log_level: str = Field(default="INFO")
    retention_run_interval_seconds: int = Field(
        default=3600,
        ge=300,
        le=86400,
        description="How often retention cleanup runs",
    )

    # Snapshot write-gating settings
    snapshot_min_write_interval_seconds: float = Field(
        default=5.0,
        ge=0.5,
        le=300.0,
        description="Minimum interval between snapshot writes per token",
    )
    snapshot_force_write_delta_pp: float = Field(
        default=0.5,
        ge=0.0,
        le=50.0,
        description="Force write when absolute price move exceeds this threshold (percentage points)",
    )

    # Table-specific retention settings (days)
    snapshot_retention_days: int = Field(default=3, ge=1, le=365)
    ohlc_1m_retention_days: int = Field(default=14, ge=1, le=365)
    ohlc_1h_retention_days: int = Field(default=120, ge=7, le=3650)
    movers_cache_retention_days: int = Field(default=14, ge=1, le=365)
    alerts_retention_days: int = Field(default=30, ge=1, le=3650)
    volume_spikes_retention_days: int = Field(default=30, ge=1, le=3650)
    arbitrage_retention_days: int = Field(default=14, ge=1, le=365)
    volume_hourly_retention_days: int = Field(default=120, ge=7, le=3650)
    storage_metrics_interval_seconds: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="Interval for emitting storage telemetry snapshots",
    )
    volume_wss_stale_after_seconds: int = Field(
        default=600,
        ge=60,
        le=86400,
        description="Maximum age for WSS-derived volume before it is treated as stale",
    )
    volume_provider_stale_after_seconds: int = Field(
        default=7200,
        ge=300,
        le=604800,
        description="Maximum age for provider-derived volume before it is treated as stale",
    )

    # Instant mover settings
    instant_mover_threshold_pp: float = Field(
        default=5.0,
        description="Minimum price move (percentage points) for instant mover alerts",
    )
    instant_mover_min_quality_score: float = Field(
        default=1.0,
        description="Minimum quality score for instant mover alerts",
    )
    instant_mover_debounce_seconds: float = Field(
        default=10.0,
        description="Per-token cooldown window for instant mover alerts",
    )
    instant_mover_min_volume: float = Field(
        default=0.0,
        description="Minimum volume required to consider instant mover alerts (0 disables)",
    )
    signal_hold_zone_enabled: bool = Field(
        default=True,
        description="Suppress borderline mover/alert signals near thresholds while keeping ranking unchanged",
    )
    signal_hold_zone_move_pp: float = Field(
        default=0.5,
        ge=0.0,
        le=50.0,
        description="Minimum move edge (pp above threshold) required to clear hold zone",
    )
    signal_hold_zone_quality_score: float = Field(
        default=0.5,
        ge=0.0,
        le=100.0,
        description="Minimum quality-score edge required to clear hold zone",
    )
    signal_hold_zone_spike_ratio: float = Field(
        default=0.25,
        ge=0.0,
        le=50.0,
        description="Minimum spike-ratio edge required to clear hold zone",
    )
    model_feature_manifest_path: str = Field(
        default="packages/core/analytics/mover_feature_manifest.json",
        description="Path to the training feature manifest used for strict inference validation",
    )
    model_feature_manifest_strict: bool = Field(
        default=True,
        description="Fail scoring/inference when live feature columns/order/types diverge from manifest",
    )
    model_scoring_interval_seconds: int = Field(
        default=86400,
        ge=3600,
        le=604800,
        description="Interval for daily resolved-market scoring updates",
    )
    model_scoring_initial_delay_seconds: int = Field(
        default=900,
        ge=0,
        le=21600,
        description="Initial delay before first resolved-market scoring run",
    )
    model_scoring_calibration_bins: int = Field(
        default=10,
        ge=4,
        le=20,
        description="Number of probability bins to compute for calibration diagnostics",
    )

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
    polymarket_subscription_refresh_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Refresh interval for Polymarket active subscriptions",
    )
    polymarket_full_metadata_sync_enabled: bool = Field(
        default=True,
        description="Run periodic full Polymarket metadata refresh for link/category correctness",
    )
    polymarket_full_metadata_sync_interval_seconds: int = Field(
        default=86400,
        ge=3600,
        le=604800,
        description="Interval for full Polymarket metadata refresh",
    )
    polymarket_full_metadata_max_markets: int = Field(
        default=20000,
        ge=500,
        le=100000,
        description="Maximum number of Polymarket markets to scan during full metadata refresh",
    )
    
    # Kalshi WebSocket Settings
    kalshi_use_wss: bool = Field(
        default=False,
        description="Enable Kalshi WebSocket for real-time updates (requires API key)"
    )
    kalshi_wss_enabled: bool = Field(
        default=False,
        description="Alias for kalshi_use_wss"
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
