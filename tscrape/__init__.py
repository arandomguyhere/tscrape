"""
TScrape - Modern Telegram Channel Scraper (2026)

A high-performance Telegram scraper combining the best practices from:
- unnohwn/telegram-scraper (resume support, parallel media)
- ergoncugler/web-scraping-telegram (Parquet storage, analytics)
- Telethon best practices (FloodWait handling, session management)
- Proxy-Hound & SOCKS5-Scanner (proxy rotation, IP protection)
- Academic papers: TelegramScrap, CTI Dataset Construction, PLOS ONE

Features:
- Incremental scraping with resume support
- Parquet + SQLite storage
- Parallel media downloads
- FloodWait handling with exponential backoff
- Rich metadata capture (reactions, views, forwards, comments)
- Export to JSON/CSV/Parquet
- Proxy rotation with health tracking
- Channel discovery via forwards (snowballing method)
- Keyword filtering with regex support
- Network graph export (GraphML/GEXF)
- Academic-grade bias tracking (gap detection, deletion tracking)
- Scrape run manifests for reproducibility
- Pluggable backends (API or web HTML scraping)
"""

__version__ = "1.4.0"
__author__ = "TScrape"

from .scraper import TelegramScraper
from .storage import StorageManager
from .media import MediaDownloader
from .proxy import ProxyManager, ProxyInfo, ProxyType
from .discovery import ChannelDiscovery, DiscoveredChannel, ChannelEdge
from .filters import MessageFilter, FilterMode, FilterResult, KeywordSet
from .bias import BiasTracker, BiasMetrics, ScrapeRunManifest, MessageStatus
from .backends import (
    ScrapeBackend,
    BackendType,
    BackendCapabilities,
    TelethonBackend,
    WebHTMLBackend
)

__all__ = [
    "TelegramScraper",
    "StorageManager",
    "MediaDownloader",
    "ProxyManager",
    "ProxyInfo",
    "ProxyType",
    "ChannelDiscovery",
    "DiscoveredChannel",
    "ChannelEdge",
    "MessageFilter",
    "FilterMode",
    "FilterResult",
    "KeywordSet",
    "BiasTracker",
    "BiasMetrics",
    "ScrapeRunManifest",
    "MessageStatus",
    "ScrapeBackend",
    "BackendType",
    "BackendCapabilities",
    "TelethonBackend",
    "WebHTMLBackend"
]
