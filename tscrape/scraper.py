"""
Main Telegram Scraper Module

Uses Telethon MTProto API for reliable, full-featured scraping.
Supports proxy rotation via Proxy-Hound and SOCKS5-Scanner.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional, List, Dict, Any, Union
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import (
    Channel, Chat, User, Message, MessageMediaPhoto, MessageMediaDocument,
    MessageReactions, ReactionCount, ReactionEmoji, ReactionCustomEmoji,
    PeerChannel, PeerChat, PeerUser
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, ChannelInvalidError,
    ChatAdminRequiredError, UserBannedInChannelError
)

from .storage import StorageManager
from .media import MediaDownloader
from .models import ScrapedMessage, ChannelInfo, ScrapeState
from .config import Config
from .proxy import ProxyManager, ProxyInfo, ProxyType

logger = logging.getLogger(__name__)


class TelegramScraper:
    """
    High-performance Telegram channel scraper.

    Features:
    - Incremental scraping with checkpoint/resume
    - FloodWait handling with exponential backoff
    - Rich metadata capture (reactions, views, forwards)
    - Parallel media downloads
    - Multiple export formats (Parquet, JSON, CSV)
    - Proxy rotation support (Proxy-Hound, SOCKS5-Scanner)
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_name: str = "tscrape_session",
        data_dir: str = "./data",
        config: Optional[Config] = None,
        proxy_manager: Optional[ProxyManager] = None
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.data_dir = Path(data_dir)
        self.config = config or Config()
        self.proxy_manager = proxy_manager

        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.client: Optional[TelegramClient] = None
        self.storage: Optional[StorageManager] = None
        self.media_downloader: Optional[MediaDownloader] = None

        self._flood_wait_count = 0
        self._messages_scraped = 0
        self._is_running = False
        self._current_proxy: Optional[ProxyInfo] = None
        self._proxy_rotation_on_flood = True

    async def connect(self, proxy: Optional[ProxyInfo] = None) -> None:
        """
        Initialize and connect the Telegram client.

        Args:
            proxy: Optional proxy to use for connection
        """
        session_path = self.data_dir / self.session_name

        # Get proxy from manager if not provided
        if proxy is None and self.proxy_manager:
            proxy = await self.proxy_manager.get_proxy()
            if proxy:
                logger.info(f"Using proxy: {proxy.host}:{proxy.port} ({proxy.proxy_type.value})")

        self._current_proxy = proxy

        # Build client with or without proxy
        client_kwargs = {
            "session": str(session_path),
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "system_version": "4.16.30-vxTSCRAPE",
            "device_model": "TScrape Bot",
            "app_version": "1.0.0"
        }

        if proxy:
            client_kwargs["proxy"] = proxy.to_telethon_proxy()

        self.client = TelegramClient(**client_kwargs)

        try:
            await self.client.start()

            me = await self.client.get_me()
            logger.info(f"Connected as: {me.username or me.phone}")

            # Report proxy success
            if proxy and self.proxy_manager:
                self.proxy_manager.report_success(proxy)

        except Exception as e:
            # Report proxy failure and retry with another
            if proxy and self.proxy_manager:
                self.proxy_manager.report_failure(proxy, str(e))
                logger.warning(f"Proxy failed: {proxy.host}:{proxy.port} - {e}")

                # Try another proxy
                new_proxy = await self.proxy_manager.get_proxy()
                if new_proxy and new_proxy != proxy:
                    logger.info(f"Retrying with proxy: {new_proxy.host}:{new_proxy.port}")
                    await self.connect(new_proxy)
                    return

            raise

        self.storage = StorageManager(self.data_dir)
        self.media_downloader = MediaDownloader(
            self.client,
            self.data_dir,
            max_concurrent=self.config.media_concurrent_downloads
        )

    async def disconnect(self) -> None:
        """Disconnect the client cleanly."""
        if self.client:
            await self.client.disconnect()
            logger.info("Disconnected from Telegram")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def get_channel_info(self, channel: Union[str, int]) -> ChannelInfo:
        """Get detailed information about a channel."""
        entity = await self.client.get_entity(channel)

        if isinstance(entity, Channel):
            full = await self.client(GetFullChannelRequest(entity))
            return ChannelInfo(
                id=entity.id,
                username=entity.username,
                title=entity.title,
                about=full.full_chat.about,
                participants_count=full.full_chat.participants_count,
                is_megagroup=entity.megagroup,
                is_broadcast=entity.broadcast,
                created_at=entity.date,
                scraped_at=datetime.now(timezone.utc)
            )
        else:
            return ChannelInfo(
                id=entity.id,
                username=getattr(entity, 'username', None),
                title=getattr(entity, 'title', str(entity.id)),
                about=None,
                participants_count=None,
                is_megagroup=False,
                is_broadcast=False,
                created_at=getattr(entity, 'date', None),
                scraped_at=datetime.now(timezone.utc)
            )

    async def scrape_channel(
        self,
        channel: Union[str, int],
        limit: Optional[int] = None,
        offset_id: int = 0,
        min_id: int = 0,
        max_id: int = 0,
        offset_date: Optional[datetime] = None,
        download_media: bool = False,
        resume: bool = True,
        progress_callback: Optional[callable] = None
    ) -> AsyncGenerator[ScrapedMessage, None]:
        """
        Scrape messages from a Telegram channel.

        Args:
            channel: Channel username or ID
            limit: Maximum number of messages to scrape (None = all)
            offset_id: Start from this message ID
            min_id: Don't fetch messages older than this ID
            max_id: Don't fetch messages newer than this ID
            offset_date: Start from this date
            download_media: Whether to download media files
            resume: Resume from last checkpoint if available
            progress_callback: Called with (scraped_count, total) periodically

        Yields:
            ScrapedMessage objects
        """
        self._is_running = True
        self._messages_scraped = 0

        try:
            entity = await self.client.get_entity(channel)
            channel_id = entity.id
            channel_name = getattr(entity, 'username', None) or str(channel_id)

            logger.info(f"Starting scrape of channel: {channel_name} (ID: {channel_id})")

            # Check for resume state
            if resume:
                state = self.storage.get_scrape_state(channel_id)
                if state and state.last_message_id > 0:
                    if min_id == 0 or state.last_message_id > min_id:
                        min_id = state.last_message_id
                        logger.info(f"Resuming from message ID: {min_id}")

            # Initialize channel storage
            self.storage.init_channel(channel_id, channel_name)

            batch = []
            batch_size = self.config.batch_size

            async for message in self.client.iter_messages(
                entity,
                limit=limit,
                offset_id=offset_id,
                min_id=min_id,
                max_id=max_id,
                offset_date=offset_date,
                wait_time=self.config.iter_wait_time
            ):
                if not self._is_running:
                    logger.info("Scrape stopped by user")
                    break

                try:
                    scraped = await self._process_message(message, channel_id, channel_name)

                    if scraped:
                        batch.append(scraped)
                        self._messages_scraped += 1

                        # Download media if requested
                        if download_media and message.media:
                            await self.media_downloader.queue_download(
                                message, channel_name
                            )

                        yield scraped

                        # Save batch periodically
                        if len(batch) >= batch_size:
                            await self._save_batch(channel_id, batch)
                            batch = []

                            if progress_callback:
                                progress_callback(self._messages_scraped, limit)

                except FloodWaitError as e:
                    await self._handle_flood_wait(e)
                    continue

                except Exception as e:
                    logger.error(f"Error processing message {message.id}: {e}")
                    continue

            # Save remaining batch
            if batch:
                await self._save_batch(channel_id, batch)

            # Wait for media downloads to complete
            if download_media:
                await self.media_downloader.wait_completion()

            logger.info(f"Scrape complete. Total messages: {self._messages_scraped}")

        except ChannelPrivateError:
            logger.error(f"Channel {channel} is private and you're not a member")
            raise
        except ChannelInvalidError:
            logger.error(f"Channel {channel} is invalid")
            raise
        except Exception as e:
            logger.error(f"Scrape failed: {e}")
            raise
        finally:
            self._is_running = False

    async def _process_message(
        self,
        message: Message,
        channel_id: int,
        channel_name: str
    ) -> Optional[ScrapedMessage]:
        """Process a single message into a ScrapedMessage object."""
        if message is None or message.action is not None:
            return None  # Skip service messages

        # Extract reactions
        reactions = []
        if message.reactions:
            for reaction in message.reactions.results:
                emoji = None
                if isinstance(reaction.reaction, ReactionEmoji):
                    emoji = reaction.reaction.emoticon
                elif isinstance(reaction.reaction, ReactionCustomEmoji):
                    emoji = f"custom:{reaction.reaction.document_id}"

                if emoji:
                    reactions.append({
                        "emoji": emoji,
                        "count": reaction.count
                    })

        # Extract reply info
        reply_to_id = None
        if message.reply_to:
            reply_to_id = message.reply_to.reply_to_msg_id

        # Extract sender info
        sender_id = None
        sender_username = None
        if message.sender:
            sender_id = message.sender.id
            sender_username = getattr(message.sender, 'username', None)

        # Extract media type
        media_type = None
        if message.media:
            media_type = type(message.media).__name__

        return ScrapedMessage(
            message_id=message.id,
            channel_id=channel_id,
            channel_name=channel_name,
            date=message.date,
            text=message.text or "",
            raw_text=message.raw_text or "",
            sender_id=sender_id,
            sender_username=sender_username,
            views=message.views or 0,
            forwards=message.forwards or 0,
            replies_count=message.replies.replies if message.replies else 0,
            reactions=reactions,
            reply_to_id=reply_to_id,
            media_type=media_type,
            has_media=message.media is not None,
            is_pinned=message.pinned,
            edit_date=message.edit_date,
            grouped_id=message.grouped_id,
            scraped_at=datetime.now(timezone.utc)
        )

    async def _save_batch(self, channel_id: int, batch: List[ScrapedMessage]) -> None:
        """Save a batch of messages and update checkpoint."""
        if not batch:
            return

        self.storage.save_messages(channel_id, batch)

        # Update checkpoint with oldest message ID in batch
        oldest_id = min(m.message_id for m in batch)
        self.storage.update_scrape_state(
            channel_id,
            last_message_id=oldest_id,
            messages_scraped=self._messages_scraped
        )

        logger.debug(f"Saved batch of {len(batch)} messages, checkpoint: {oldest_id}")

    async def _handle_flood_wait(self, error: FloodWaitError) -> None:
        """Handle Telegram's FloodWait with exponential backoff and optional proxy rotation."""
        wait_time = error.seconds
        self._flood_wait_count += 1

        # Add extra backoff based on flood count
        extra_wait = min(self._flood_wait_count * 5, 60)
        total_wait = wait_time + extra_wait

        logger.warning(
            f"FloodWait: sleeping {total_wait}s "
            f"(base: {wait_time}s, backoff: {extra_wait}s, count: {self._flood_wait_count})"
        )

        # Try rotating proxy on repeated flood waits
        if self._proxy_rotation_on_flood and self.proxy_manager and self._flood_wait_count >= 2:
            if self._current_proxy:
                self.proxy_manager.report_failure(self._current_proxy, "FloodWait")

            new_proxy = await self.proxy_manager.get_proxy()
            if new_proxy and new_proxy != self._current_proxy:
                logger.info(f"Rotating to new proxy: {new_proxy.host}:{new_proxy.port}")
                await self._reconnect_with_proxy(new_proxy)
                # Reduced wait after proxy rotation
                total_wait = min(total_wait, 30)

        await asyncio.sleep(total_wait)

    async def _reconnect_with_proxy(self, proxy: ProxyInfo) -> None:
        """Reconnect with a new proxy."""
        try:
            if self.client:
                await self.client.disconnect()

            self._current_proxy = proxy
            session_path = self.data_dir / self.session_name

            self.client = TelegramClient(
                str(session_path),
                self.api_id,
                self.api_hash,
                system_version="4.16.30-vxTSCRAPE",
                device_model="TScrape Bot",
                app_version="1.0.0",
                proxy=proxy.to_telethon_proxy()
            )

            await self.client.start()
            self._flood_wait_count = 0  # Reset flood count on new proxy
            logger.info(f"Reconnected with proxy: {proxy.host}:{proxy.port}")

            if self.proxy_manager:
                self.proxy_manager.report_success(proxy)

        except Exception as e:
            logger.error(f"Failed to reconnect with proxy: {e}")
            if self.proxy_manager:
                self.proxy_manager.report_failure(proxy, str(e))

    def stop(self) -> None:
        """Signal the scraper to stop gracefully."""
        self._is_running = False
        logger.info("Stop signal received")

    async def get_dialogs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get list of accessible channels/groups."""
        dialogs = []

        async for dialog in self.client.iter_dialogs(limit=limit):
            if dialog.is_channel or dialog.is_group:
                dialogs.append({
                    "id": dialog.id,
                    "name": dialog.name,
                    "username": getattr(dialog.entity, 'username', None),
                    "is_channel": dialog.is_channel,
                    "is_group": dialog.is_group,
                    "unread_count": dialog.unread_count
                })

        return dialogs

    def get_proxy_stats(self) -> Optional[Dict[str, Any]]:
        """Get proxy pool statistics."""
        if self.proxy_manager:
            stats = self.proxy_manager.get_stats()
            stats["current_proxy"] = (
                f"{self._current_proxy.host}:{self._current_proxy.port}"
                if self._current_proxy else None
            )
            return stats
        return None

    def set_proxy_rotation(self, enabled: bool) -> None:
        """Enable or disable proxy rotation on FloodWait."""
        self._proxy_rotation_on_flood = enabled
        logger.info(f"Proxy rotation on FloodWait: {'enabled' if enabled else 'disabled'}")
