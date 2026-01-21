"""
Configuration for TScrape.

Supports loading from environment variables and config files.
"""

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List


@dataclass
class Config:
    """
    TScrape configuration with sensible defaults.

    All timing values are tuned to respect Telegram's rate limits
    while maintaining good performance.
    """

    # API credentials (required)
    api_id: Optional[int] = None
    api_hash: Optional[str] = None

    # Session settings
    session_name: str = "tscrape_session"
    data_dir: str = "./data"

    # Scraping settings
    batch_size: int = 100  # Messages per batch write
    iter_wait_time: int = 2  # Seconds between message batches
    max_retries: int = 3  # Retries on transient errors

    # Media settings
    media_concurrent_downloads: int = 3  # Parallel downloads
    download_photos: bool = True
    download_videos: bool = True
    download_documents: bool = True
    max_media_size_mb: int = 100  # Skip files larger than this

    # Rate limiting
    flood_wait_multiplier: float = 1.5  # Extra wait on flood
    min_request_delay: float = 0.5  # Minimum delay between requests

    # Export settings
    default_export_format: str = "parquet"  # parquet, json, csv
    compression: str = "snappy"  # For Parquet files

    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            api_id=int(os.getenv("TELEGRAM_API_ID", 0)) or None,
            api_hash=os.getenv("TELEGRAM_API_HASH"),
            session_name=os.getenv("TSCRAPE_SESSION", "tscrape_session"),
            data_dir=os.getenv("TSCRAPE_DATA_DIR", "./data"),
            batch_size=int(os.getenv("TSCRAPE_BATCH_SIZE", 100)),
            media_concurrent_downloads=int(os.getenv("TSCRAPE_MEDIA_CONCURRENT", 3)),
            log_level=os.getenv("TSCRAPE_LOG_LEVEL", "INFO"),
        )

    @classmethod
    def from_file(cls, path: Path) -> "Config":
        """Load configuration from a JSON file."""
        with open(path) as f:
            data = json.load(f)
        return cls(**data)

    def to_file(self, path: Path) -> None:
        """Save configuration to a JSON file."""
        data = {
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "session_name": self.session_name,
            "data_dir": self.data_dir,
            "batch_size": self.batch_size,
            "iter_wait_time": self.iter_wait_time,
            "max_retries": self.max_retries,
            "media_concurrent_downloads": self.media_concurrent_downloads,
            "download_photos": self.download_photos,
            "download_videos": self.download_videos,
            "download_documents": self.download_documents,
            "max_media_size_mb": self.max_media_size_mb,
            "default_export_format": self.default_export_format,
            "compression": self.compression,
            "log_level": self.log_level,
            "log_file": self.log_file,
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    def validate(self) -> List[str]:
        """Validate configuration, returns list of errors."""
        errors = []

        if not self.api_id:
            errors.append("api_id is required (set TELEGRAM_API_ID)")
        if not self.api_hash:
            errors.append("api_hash is required (set TELEGRAM_API_HASH)")
        if self.batch_size < 1:
            errors.append("batch_size must be >= 1")
        if self.media_concurrent_downloads < 1:
            errors.append("media_concurrent_downloads must be >= 1")

        return errors
