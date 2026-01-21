"""
Media Downloader for TScrape.

Features:
- Parallel downloads with configurable concurrency
- Automatic retry with exponential backoff
- Progress tracking
- File deduplication
- Support for photos, videos, documents, audio
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Set
from dataclasses import dataclass

from telethon import TelegramClient
from telethon.tl.types import (
    Message, MessageMediaPhoto, MessageMediaDocument,
    MessageMediaWebPage, DocumentAttributeFilename,
    DocumentAttributeVideo, DocumentAttributeAudio
)
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)


@dataclass
class DownloadTask:
    """Represents a media download task."""
    message: Message
    channel_name: str
    priority: int = 0


class MediaDownloader:
    """
    Parallel media downloader with queue management.

    Features:
    - Concurrent downloads (configurable)
    - Automatic retry with backoff
    - Progress callbacks
    - File naming with message ID for traceability
    """

    def __init__(
        self,
        client: TelegramClient,
        data_dir: Path,
        max_concurrent: int = 3,
        max_retries: int = 3
    ):
        self.client = client
        self.data_dir = Path(data_dir)
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries

        self._queue: asyncio.Queue[DownloadTask] = asyncio.Queue()
        self._workers: list[asyncio.Task] = []
        self._downloaded_count = 0
        self._failed_count = 0
        self._is_running = False
        self._downloaded_hashes: Set[str] = set()

        # Semaphore for concurrency control
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def start_workers(self) -> None:
        """Start download worker tasks."""
        if self._is_running:
            return

        self._is_running = True
        self._workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.max_concurrent)
        ]
        logger.info(f"Started {self.max_concurrent} download workers")

    async def stop_workers(self) -> None:
        """Stop all workers gracefully."""
        self._is_running = False

        # Signal workers to stop
        for _ in range(self.max_concurrent):
            await self._queue.put(None)

        # Wait for workers to finish
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers = []

        logger.info("Download workers stopped")

    async def queue_download(self, message: Message, channel_name: str) -> None:
        """Add a message to the download queue."""
        if not message.media:
            return

        # Skip unsupported media types
        if isinstance(message.media, MessageMediaWebPage):
            return

        # Start workers if not running
        if not self._is_running:
            await self.start_workers()

        task = DownloadTask(message=message, channel_name=channel_name)
        await self._queue.put(task)

    async def _worker(self, worker_id: int) -> None:
        """Worker coroutine that processes download tasks."""
        logger.debug(f"Worker {worker_id} started")

        while self._is_running:
            try:
                task = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=1.0
                )

                if task is None:  # Shutdown signal
                    break

                async with self._semaphore:
                    await self._download_with_retry(task, worker_id)

                self._queue.task_done()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")

        logger.debug(f"Worker {worker_id} stopped")

    async def _download_with_retry(self, task: DownloadTask, worker_id: int) -> None:
        """Download media with retry logic."""
        for attempt in range(self.max_retries):
            try:
                await self._download_media(task)
                self._downloaded_count += 1
                return

            except FloodWaitError as e:
                wait_time = e.seconds + (attempt * 5)
                logger.warning(
                    f"Worker {worker_id}: FloodWait, sleeping {wait_time}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                await asyncio.sleep(wait_time)

            except Exception as e:
                wait_time = (2 ** attempt) * 2  # Exponential backoff
                logger.warning(
                    f"Worker {worker_id}: Download failed: {e}, "
                    f"retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries})"
                )
                await asyncio.sleep(wait_time)

        self._failed_count += 1
        logger.error(f"Failed to download media for message {task.message.id} after {self.max_retries} attempts")

    async def _download_media(self, task: DownloadTask) -> Optional[Path]:
        """Download media from a message."""
        message = task.message
        channel_name = task.channel_name

        # Determine output directory
        media_dir = self.data_dir / channel_name / "media"
        media_dir.mkdir(parents=True, exist_ok=True)

        # Get file info
        file_info = self._get_file_info(message)
        if not file_info:
            return None

        # Generate filename
        filename = self._generate_filename(message, file_info)
        output_path = media_dir / filename

        # Skip if already exists
        if output_path.exists():
            logger.debug(f"Skipping existing file: {filename}")
            return output_path

        # Download
        logger.debug(f"Downloading: {filename}")

        result = await self.client.download_media(
            message,
            file=str(output_path),
            progress_callback=None  # Could add progress callback here
        )

        if result:
            # Verify and deduplicate
            file_hash = self._hash_file(output_path)
            if file_hash in self._downloaded_hashes:
                logger.debug(f"Duplicate file detected, removing: {filename}")
                output_path.unlink()
                return None

            self._downloaded_hashes.add(file_hash)
            logger.info(f"Downloaded: {filename} ({output_path.stat().st_size} bytes)")
            return output_path

        return None

    def _get_file_info(self, message: Message) -> Optional[Dict[str, Any]]:
        """Extract file information from message media."""
        media = message.media

        if isinstance(media, MessageMediaPhoto):
            return {
                "type": "photo",
                "extension": "jpg",
                "mime_type": "image/jpeg"
            }

        elif isinstance(media, MessageMediaDocument):
            doc = media.document
            if not doc:
                return None

            # Get filename from attributes
            filename = None
            is_video = False
            is_audio = False

            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    filename = attr.file_name
                elif isinstance(attr, DocumentAttributeVideo):
                    is_video = True
                elif isinstance(attr, DocumentAttributeAudio):
                    is_audio = True

            # Determine type and extension
            mime_type = doc.mime_type or "application/octet-stream"

            if is_video:
                file_type = "video"
                extension = mime_type.split('/')[-1] if '/' in mime_type else "mp4"
            elif is_audio:
                file_type = "audio"
                extension = mime_type.split('/')[-1] if '/' in mime_type else "mp3"
            elif mime_type.startswith("image/"):
                file_type = "image"
                extension = mime_type.split('/')[-1]
            else:
                file_type = "document"
                extension = filename.split('.')[-1] if filename and '.' in filename else "bin"

            return {
                "type": file_type,
                "extension": extension,
                "mime_type": mime_type,
                "original_filename": filename,
                "size": doc.size
            }

        return None

    def _generate_filename(self, message: Message, file_info: Dict[str, Any]) -> str:
        """Generate a unique filename for the media."""
        msg_id = message.id
        date_str = message.date.strftime("%Y%m%d_%H%M%S")
        media_type = file_info["type"]
        extension = file_info["extension"]

        # Clean extension
        extension = extension.lower().replace("jpeg", "jpg")

        return f"{date_str}_{msg_id}_{media_type}.{extension}"

    def _hash_file(self, file_path: Path, chunk_size: int = 8192) -> str:
        """Calculate MD5 hash of a file for deduplication."""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    async def wait_completion(self) -> None:
        """Wait for all queued downloads to complete."""
        await self._queue.join()
        await self.stop_workers()

    def get_stats(self) -> Dict[str, int]:
        """Get download statistics."""
        return {
            "downloaded": self._downloaded_count,
            "failed": self._failed_count,
            "pending": self._queue.qsize()
        }
