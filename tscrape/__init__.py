"""
TScrape - Modern Telegram Channel Scraper (2026)

A high-performance Telegram scraper combining the best practices from:
- unnohwn/telegram-scraper (resume support, parallel media)
- ergoncugler/web-scraping-telegram (Parquet storage, analytics)
- Telethon best practices (FloodWait handling, session management)
- Proxy-Hound & SOCKS5-Scanner (proxy rotation, IP protection)

Features:
- Incremental scraping with resume support
- Parquet + SQLite storage
- Parallel media downloads
- FloodWait handling with exponential backoff
- Rich metadata capture (reactions, views, forwards, comments)
- Export to JSON/CSV/Parquet
- Proxy rotation with health tracking
"""

__version__ = "1.1.0"
__author__ = "TScrape"

from .scraper import TelegramScraper
from .storage import StorageManager
from .media import MediaDownloader
from .proxy import ProxyManager, ProxyInfo, ProxyType

__all__ = [
    "TelegramScraper",
    "StorageManager",
    "MediaDownloader",
    "ProxyManager",
    "ProxyInfo",
    "ProxyType"
]
