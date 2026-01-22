"""
TScrape Backend System

Pluggable backends for different scraping strategies:
- TelethonBackend: Full API access (default, reference standard)
- WebHTMLBackend: HTML scraping via t.me (no API, free)
- RSSBackend: RSS mirror ingestion (lowest fidelity)
"""

from .base import ScrapeBackend, BackendCapabilities, BackendType
from .telethon_backend import TelethonBackend
from .web_backend import WebHTMLBackend

__all__ = [
    "ScrapeBackend",
    "BackendCapabilities",
    "BackendType",
    "TelethonBackend",
    "WebHTMLBackend"
]
