"""
Web HTML Backend - No API Required

Scrapes public Telegram channels via t.me/s/channelname
without using the official API.

Use cases:
- No Telegram account available
- Account risk mitigation
- Quick OSINT monitoring
- Free, no credentials needed

Limitations (important):
- Public channels only
- No reactions, accurate views, or replies
- Message IDs inferred (not authoritative)
- No edit/deletion detection
- Limited forward metadata
- Rate limited by Cloudflare

This backend explicitly downgrades bias confidence
and discloses limitations in all exports.
"""

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Optional, Dict, Any, List
from dataclasses import dataclass

try:
    import aiohttp
    from aiohttp import ClientSession, ClientTimeout
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

from .base import (
    ScrapeBackend,
    BackendType,
    BackendCapabilities,
    ScrapedItem
)

logger = logging.getLogger(__name__)


# Web HTML capabilities - significantly reduced
WEB_HTML_CAPABILITIES = BackendCapabilities(
    public_channels=True,
    private_channels=False,
    message_text=True,
    message_timestamps=True,
    views=False,  # Not reliably available
    forwards=False,
    reactions=False,
    replies=False,
    sender_info=False,
    media_urls=True,  # URLs available, not download
    media_download=False,
    resume_by_id=False,  # IDs not reliable
    edit_detection=False,
    deletion_detection=False,
    message_id_reliable=False,
    forward_source_detection=True,  # Best effort
    snowball_discovery=True,  # Limited
    bias_tracking_supported=False,
    bias_confidence="low"
)


@dataclass
class ParsedMessage:
    """Parsed message from HTML."""
    text: str
    timestamp: Optional[datetime]
    message_id: Optional[int]
    forward_from: Optional[str]
    media_urls: List[str]
    views_text: Optional[str]


class WebHTMLBackend(ScrapeBackend):
    """
    HTML scraping backend for public Telegram channels.

    Scrapes https://t.me/s/channelname which provides
    a public web view of channel messages.

    Usage:
        async with WebHTMLBackend(data_dir="./data") as backend:
            async for item in backend.scrape_channel("channelname"):
                print(item.text)

    Note: This backend has significant limitations compared
    to the Telethon API backend. Use for monitoring and OSINT,
    not archival research.
    """

    BASE_URL = "https://t.me/s/{channel}"
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    def __init__(
        self,
        data_dir: Path = Path("./data"),
        request_delay: float = 2.0,
        timeout: int = 30,
        max_retries: int = 3
    ):
        super().__init__(data_dir)
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self._session: Optional[ClientSession] = None
        self._messages_scraped = 0

        if not AIOHTTP_AVAILABLE:
            raise ImportError(
                "aiohttp is required for WebHTMLBackend. "
                "Install with: pip install aiohttp"
            )

    @property
    def backend_type(self) -> BackendType:
        return BackendType.WEB_HTML

    @property
    def capabilities(self) -> BackendCapabilities:
        return WEB_HTML_CAPABILITIES

    async def connect(self) -> None:
        """Initialize HTTP session."""
        timeout = ClientTimeout(total=self.timeout)
        self._session = ClientSession(
            timeout=timeout,
            headers={"User-Agent": self.USER_AGENT}
        )
        logger.info("WebHTMLBackend connected")

    async def disconnect(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("WebHTMLBackend disconnected")

    async def scrape_channel(
        self,
        channel: str,
        limit: Optional[int] = None,
        **kwargs
    ) -> AsyncGenerator[ScrapedItem, None]:
        """
        Scrape public channel via web interface.

        Args:
            channel: Channel username (without @)
            limit: Maximum messages to scrape

        Yields:
            ScrapedItem with available data
        """
        if not self._session:
            raise RuntimeError("Backend not connected")

        # Normalize channel name
        channel = channel.lstrip("@")

        self._messages_scraped = 0
        before_id = None
        seen_ids = set()

        logger.info(f"Starting web scrape of {channel}")

        while True:
            # Build URL
            url = self.BASE_URL.format(channel=channel)
            if before_id:
                url += f"?before={before_id}"

            # Fetch page
            html = await self._fetch_page(url)
            if not html:
                logger.warning("Failed to fetch page, stopping")
                break

            # Parse messages
            messages = self._parse_messages(html, channel)

            if not messages:
                logger.info("No more messages found")
                break

            # Track for pagination
            oldest_id = None

            for msg in messages:
                # Skip duplicates
                msg_hash = self._message_hash(msg)
                if msg_hash in seen_ids:
                    continue
                seen_ids.add(msg_hash)

                # Track oldest for pagination
                if msg.message_id:
                    if oldest_id is None or msg.message_id < oldest_id:
                        oldest_id = msg.message_id

                # Yield item
                yield ScrapedItem(
                    text=msg.text,
                    timestamp=msg.timestamp,
                    message_id=msg.message_id,
                    channel_name=channel,
                    forward_from=msg.forward_from,
                    media_urls=msg.media_urls,
                    has_media=bool(msg.media_urls),
                    backend=self.backend_type.value,
                    scraped_at=datetime.now(timezone.utc)
                )

                self._messages_scraped += 1

                if limit and self._messages_scraped >= limit:
                    logger.info(f"Reached limit of {limit} messages")
                    return

            # Set up pagination
            if oldest_id:
                before_id = oldest_id
            else:
                break

            # Rate limiting
            await asyncio.sleep(self.request_delay)

        logger.info(f"Web scrape complete: {self._messages_scraped} messages")

    async def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with retry logic."""
        for attempt in range(self.max_retries):
            try:
                async with self._session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 404:
                        logger.error(f"Channel not found: {url}")
                        return None
                    elif response.status == 429:
                        wait = 30 * (attempt + 1)
                        logger.warning(f"Rate limited, waiting {wait}s")
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")

            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {url}, attempt {attempt + 1}")
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")

            if attempt < self.max_retries - 1:
                await asyncio.sleep(2 ** attempt)

        return None

    def _parse_messages(self, html: str, channel: str) -> List[ParsedMessage]:
        """
        Parse messages from HTML.

        This uses regex patterns to extract data from the
        t.me/s/ page structure. May break if Telegram changes
        their HTML format.
        """
        messages = []

        # Pattern for message blocks
        # t.me/s/ uses a specific HTML structure
        message_pattern = re.compile(
            r'<div class="tgme_widget_message_wrap[^"]*"[^>]*data-post="([^"]+)"',
            re.DOTALL
        )

        # Find all message containers
        for match in message_pattern.finditer(html):
            post_id = match.group(1)  # e.g., "channelname/12345"

            # Extract message ID
            msg_id = None
            if "/" in post_id:
                try:
                    msg_id = int(post_id.split("/")[-1])
                except ValueError:
                    pass

            # Find the message content area
            start = match.start()
            # Look for the next message or end
            next_match = message_pattern.search(html, match.end())
            end = next_match.start() if next_match else len(html)
            msg_html = html[start:end]

            # Extract text
            text = self._extract_text(msg_html)

            # Extract timestamp
            timestamp = self._extract_timestamp(msg_html)

            # Extract forward info
            forward_from = self._extract_forward(msg_html)

            # Extract media URLs
            media_urls = self._extract_media_urls(msg_html)

            # Extract views (best effort)
            views_text = self._extract_views(msg_html)

            if text or media_urls:
                messages.append(ParsedMessage(
                    text=text,
                    timestamp=timestamp,
                    message_id=msg_id,
                    forward_from=forward_from,
                    media_urls=media_urls,
                    views_text=views_text
                ))

        return messages

    def _extract_text(self, html: str) -> str:
        """Extract message text from HTML block."""
        # Look for message text container
        text_match = re.search(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            html,
            re.DOTALL
        )
        if text_match:
            text = text_match.group(1)
            # Clean HTML tags
            text = re.sub(r'<br\s*/?>', '\n', text)
            text = re.sub(r'<[^>]+>', '', text)
            # Decode entities
            text = text.replace('&amp;', '&')
            text = text.replace('&lt;', '<')
            text = text.replace('&gt;', '>')
            text = text.replace('&quot;', '"')
            text = text.replace('&#39;', "'")
            text = text.replace('&nbsp;', ' ')
            return text.strip()
        return ""

    def _extract_timestamp(self, html: str) -> Optional[datetime]:
        """Extract timestamp from HTML block."""
        # Look for datetime attribute
        time_match = re.search(
            r'<time[^>]*datetime="([^"]+)"',
            html
        )
        if time_match:
            try:
                ts_str = time_match.group(1)
                # Handle ISO format
                return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            except ValueError:
                pass
        return None

    def _extract_forward(self, html: str) -> Optional[str]:
        """Extract forward source from HTML block."""
        # Look for forwarded from indicator
        fwd_match = re.search(
            r'<a class="tgme_widget_message_forwarded_from_name"[^>]*href="https://t\.me/([^"]+)"',
            html
        )
        if fwd_match:
            return fwd_match.group(1).split("/")[0]  # Get channel name
        return None

    def _extract_media_urls(self, html: str) -> List[str]:
        """Extract media URLs from HTML block."""
        urls = []

        # Photos
        photo_matches = re.findall(
            r'style="background-image:url\(\'([^\']+)\'\)"',
            html
        )
        urls.extend(photo_matches)

        # Video thumbnails
        video_matches = re.findall(
            r'<video[^>]*src="([^"]+)"',
            html
        )
        urls.extend(video_matches)

        return urls

    def _extract_views(self, html: str) -> Optional[str]:
        """Extract view count text (not reliable)."""
        views_match = re.search(
            r'<span class="tgme_widget_message_views">([^<]+)</span>',
            html
        )
        if views_match:
            return views_match.group(1).strip()
        return None

    def _message_hash(self, msg: ParsedMessage) -> str:
        """Create hash for deduplication."""
        content = f"{msg.text}|{msg.timestamp}|{msg.message_id}"
        return hashlib.md5(content.encode()).hexdigest()

    async def get_channel_info(self, channel: str) -> Optional[Dict[str, Any]]:
        """Get basic channel info from web page."""
        if not self._session:
            return None

        channel = channel.lstrip("@")
        url = self.BASE_URL.format(channel=channel)

        html = await self._fetch_page(url)
        if not html:
            return None

        info = {"username": channel}

        # Extract title
        title_match = re.search(
            r'<div class="tgme_channel_info_header_title[^"]*"[^>]*>.*?<span[^>]*>([^<]+)</span>',
            html,
            re.DOTALL
        )
        if title_match:
            info["title"] = title_match.group(1).strip()

        # Extract description
        desc_match = re.search(
            r'<div class="tgme_channel_info_description[^"]*">([^<]+)</div>',
            html
        )
        if desc_match:
            info["about"] = desc_match.group(1).strip()

        # Extract member count
        members_match = re.search(
            r'<div class="tgme_channel_info_counter[^"]*">.*?<span class="counter_value">([^<]+)</span>.*?<span class="counter_type">([^<]+)</span>',
            html,
            re.DOTALL
        )
        if members_match:
            count_str = members_match.group(1).strip().replace(' ', '').replace(',', '')
            count_type = members_match.group(2).strip().lower()
            if 'subscriber' in count_type or 'member' in count_type:
                try:
                    # Handle K, M suffixes
                    if count_str.endswith('K'):
                        info["participants_count"] = int(float(count_str[:-1]) * 1000)
                    elif count_str.endswith('M'):
                        info["participants_count"] = int(float(count_str[:-1]) * 1000000)
                    else:
                        info["participants_count"] = int(count_str)
                except ValueError:
                    pass

        return info if len(info) > 1 else None

    def get_discovered_forwards(self, items: List[ScrapedItem]) -> List[str]:
        """
        Extract unique forward sources for snowball discovery.

        Limited compared to API but still useful for OSINT.
        """
        forwards = set()
        for item in items:
            if item.forward_from:
                forwards.add(item.forward_from)
        return list(forwards)
