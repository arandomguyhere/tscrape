"""
Data models for TScrape.

Using dataclasses for clean, typed data structures.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class ScrapedMessage:
    """Represents a scraped Telegram message with full metadata."""

    message_id: int
    channel_id: int
    channel_name: str
    date: datetime
    text: str
    raw_text: str

    # Sender info
    sender_id: Optional[int] = None
    sender_username: Optional[str] = None

    # Engagement metrics
    views: int = 0
    forwards: int = 0
    replies_count: int = 0
    reactions: List[Dict[str, Any]] = field(default_factory=list)

    # Reply chain
    reply_to_id: Optional[int] = None

    # Media info
    media_type: Optional[str] = None
    has_media: bool = False

    # Message metadata
    is_pinned: bool = False
    edit_date: Optional[datetime] = None
    grouped_id: Optional[int] = None

    # Scrape metadata
    scraped_at: datetime = field(default_factory=lambda: datetime.now())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime to ISO format strings
        for key in ['date', 'edit_date', 'scraped_at']:
            if data[key] is not None:
                data[key] = data[key].isoformat()
        return data

    def to_flat_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary for tabular storage."""
        data = self.to_dict()
        # Flatten reactions to JSON string
        import json
        data['reactions_json'] = json.dumps(data.pop('reactions'))
        return data


@dataclass
class ChannelInfo:
    """Information about a Telegram channel."""

    id: int
    title: str
    username: Optional[str] = None
    about: Optional[str] = None
    participants_count: Optional[int] = None
    is_megagroup: bool = False
    is_broadcast: bool = False
    created_at: Optional[datetime] = None
    scraped_at: datetime = field(default_factory=lambda: datetime.now())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        for key in ['created_at', 'scraped_at']:
            if data[key] is not None:
                data[key] = data[key].isoformat()
        return data


@dataclass
class ScrapeState:
    """Tracks scraping progress for resume capability."""

    channel_id: int
    channel_name: str
    last_message_id: int = 0
    oldest_message_id: int = 0
    messages_scraped: int = 0
    media_downloaded: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now())
    updated_at: datetime = field(default_factory=lambda: datetime.now())
    completed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        for key in ['started_at', 'updated_at']:
            if data[key] is not None:
                data[key] = data[key].isoformat()
        return data


@dataclass
class MediaFile:
    """Represents a downloaded media file."""

    message_id: int
    channel_id: int
    file_path: str
    media_type: str
    file_size: int = 0
    mime_type: Optional[str] = None
    file_name: Optional[str] = None
    downloaded_at: datetime = field(default_factory=lambda: datetime.now())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['downloaded_at'] = data['downloaded_at'].isoformat()
        return data


@dataclass
class ScrapeConfig:
    """Configuration for a scrape job."""

    channels: List[str]
    limit: Optional[int] = None
    download_media: bool = False
    media_types: List[str] = field(default_factory=lambda: ['photo', 'document', 'video'])
    min_date: Optional[datetime] = None
    max_date: Optional[datetime] = None
    resume: bool = True
    export_format: str = "parquet"  # parquet, json, csv

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        for key in ['min_date', 'max_date']:
            if data[key] is not None:
                data[key] = data[key].isoformat()
        return data
