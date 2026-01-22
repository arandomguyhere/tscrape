"""
Base Backend Abstract Class

Defines the interface that all scraping backends must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import AsyncGenerator, Optional, List, Dict, Any


class BackendType(Enum):
    """Available backend types."""
    TELETHON = "telethon"      # Full API (default)
    WEB_HTML = "web"           # HTML scraping (no API)
    RSS = "rss"                # RSS mirrors (lowest fidelity)


@dataclass
class BackendCapabilities:
    """
    Declares what a backend can and cannot do.

    Used for:
    - Feature gating at runtime
    - Bias disclosure in reports
    - User expectations management
    """
    # Core scraping
    public_channels: bool = True
    private_channels: bool = False
    message_text: bool = True
    message_timestamps: bool = True

    # Metadata
    views: bool = False
    forwards: bool = False
    reactions: bool = False
    replies: bool = False
    sender_info: bool = False

    # Media
    media_urls: bool = False
    media_download: bool = False

    # Advanced features
    resume_by_id: bool = False
    edit_detection: bool = False
    deletion_detection: bool = False
    message_id_reliable: bool = False

    # Discovery
    forward_source_detection: bool = False
    snowball_discovery: bool = False

    # Bias tracking
    bias_tracking_supported: bool = False
    bias_confidence: str = "none"  # none, low, medium, high

    def get_limitations(self) -> List[str]:
        """Return list of known limitations for disclosure."""
        limitations = []

        if not self.private_channels:
            limitations.append("Public channels only")
        if not self.message_id_reliable:
            limitations.append("Message IDs inferred, not authoritative")
        if not self.edit_detection:
            limitations.append("Edits not observable")
        if not self.deletion_detection:
            limitations.append("Deletions not detectable")
        if not self.reactions:
            limitations.append("Reactions not captured")
        if not self.views:
            limitations.append("View counts unavailable or inaccurate")
        if not self.media_download:
            limitations.append("Media download not supported")
        if not self.resume_by_id:
            limitations.append("Resume by message ID not reliable")
        if not self.sender_info:
            limitations.append("Sender information not available")

        return limitations

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "public_channels": self.public_channels,
            "private_channels": self.private_channels,
            "message_text": self.message_text,
            "message_timestamps": self.message_timestamps,
            "views": self.views,
            "forwards": self.forwards,
            "reactions": self.reactions,
            "replies": self.replies,
            "sender_info": self.sender_info,
            "media_urls": self.media_urls,
            "media_download": self.media_download,
            "resume_by_id": self.resume_by_id,
            "edit_detection": self.edit_detection,
            "deletion_detection": self.deletion_detection,
            "message_id_reliable": self.message_id_reliable,
            "forward_source_detection": self.forward_source_detection,
            "snowball_discovery": self.snowball_discovery,
            "bias_tracking_supported": self.bias_tracking_supported,
            "bias_confidence": self.bias_confidence,
            "known_limitations": self.get_limitations()
        }


@dataclass
class ScrapedItem:
    """
    Generic scraped item (backend-agnostic).

    More permissive than ScrapedMessage to handle
    varying backend capabilities.
    """
    # Required fields
    text: str
    timestamp: Optional[datetime] = None

    # Optional identifiers
    message_id: Optional[int] = None
    channel_id: Optional[int] = None
    channel_name: Optional[str] = None

    # Optional metadata
    views: Optional[int] = None
    forwards: Optional[int] = None
    forward_from: Optional[str] = None

    # Media
    media_urls: List[str] = field(default_factory=list)
    has_media: bool = False

    # Backend metadata
    backend: str = "unknown"
    scraped_at: Optional[datetime] = None
    raw_html: Optional[str] = None  # For debugging


class ScrapeBackend(ABC):
    """
    Abstract base class for all scraping backends.

    Implementations:
    - TelethonBackend: Full Telegram API (reference standard)
    - WebHTMLBackend: HTML scraping via t.me (no API needed)
    - RSSBackend: RSS mirror ingestion
    """

    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @property
    @abstractmethod
    def backend_type(self) -> BackendType:
        """Return the backend type."""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> BackendCapabilities:
        """Return backend capabilities for feature gating."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """Initialize the backend connection/session."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up the backend connection/session."""
        pass

    @abstractmethod
    async def scrape_channel(
        self,
        channel: str,
        limit: Optional[int] = None,
        **kwargs
    ) -> AsyncGenerator[ScrapedItem, None]:
        """
        Scrape messages from a channel.

        Args:
            channel: Channel username or identifier
            limit: Maximum messages to scrape
            **kwargs: Backend-specific options

        Yields:
            ScrapedItem objects
        """
        pass

    async def get_channel_info(self, channel: str) -> Optional[Dict[str, Any]]:
        """
        Get channel metadata (if supported).

        Default implementation returns None.
        Override in backends that support this.
        """
        return None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    def get_bias_disclosure(self) -> Dict[str, Any]:
        """
        Generate bias disclosure for reports.

        This should be included in any data export
        to maintain academic integrity.
        """
        return {
            "backend": self.backend_type.value,
            "bias_confidence": self.capabilities.bias_confidence,
            "capabilities": self.capabilities.to_dict(),
            "known_limitations": self.capabilities.get_limitations(),
            "disclaimer": self._get_disclaimer()
        }

    def _get_disclaimer(self) -> str:
        """Generate appropriate disclaimer based on backend type."""
        if self.backend_type == BackendType.TELETHON:
            return (
                "Data collected via official Telegram MTProto API. "
                "Subject to standard API limitations and rate controls."
            )
        elif self.backend_type == BackendType.WEB_HTML:
            return (
                "Data collected via HTML scraping of public web interface. "
                "Message IDs are inferred and may not be authoritative. "
                "Edit history, reactions, and accurate view counts are not available. "
                "This data should be treated as observational, not archival."
            )
        elif self.backend_type == BackendType.RSS:
            return (
                "Data ingested from third-party RSS feeds or mirrors. "
                "Completeness, accuracy, and timeliness cannot be guaranteed. "
                "This is secondary source data."
            )
        return "Unknown backend. Data quality unverified."
