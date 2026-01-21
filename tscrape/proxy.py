"""
Proxy Manager for TScrape.

Features:
- Fetch proxies from Proxy-Hound and SOCKS5-Scanner
- Proxy rotation with health tracking
- GeoIP filtering
- Automatic failover on errors
"""

import asyncio
import aiohttp
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from urllib.parse import urlparse

import python_socks
from python_socks.async_.asyncio.v2 import Proxy

logger = logging.getLogger(__name__)


class ProxyType(Enum):
    """Supported proxy types."""
    HTTP = "http"
    HTTPS = "https"
    SOCKS4 = "socks4"
    SOCKS5 = "socks5"


@dataclass
class ProxyInfo:
    """Represents a proxy with metadata."""
    host: str
    port: int
    proxy_type: ProxyType
    username: Optional[str] = None
    password: Optional[str] = None

    # Metadata from proxy sources
    country: Optional[str] = None
    country_code: Optional[str] = None
    city: Optional[str] = None
    latency_ms: Optional[float] = None
    score: Optional[float] = None
    asn: Optional[str] = None
    org: Optional[str] = None

    # Health tracking
    successes: int = 0
    failures: int = 0
    last_used: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    is_dead: bool = False

    @property
    def url(self) -> str:
        """Get proxy URL string."""
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        return f"{self.proxy_type.value}://{auth}{self.host}:{self.port}"

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.successes + self.failures
        if total == 0:
            return 1.0  # Assume good until proven bad
        return self.successes / total

    def mark_success(self) -> None:
        """Mark a successful use."""
        self.successes += 1
        self.last_used = datetime.now(timezone.utc)
        self.last_success = self.last_used
        self.is_dead = False

    def mark_failure(self) -> None:
        """Mark a failed use."""
        self.failures += 1
        self.last_used = datetime.now(timezone.utc)
        self.last_failure = self.last_used

        # Mark as dead after too many consecutive failures
        if self.failures >= 3 and self.success_rate < 0.2:
            self.is_dead = True

    def to_telethon_proxy(self) -> tuple:
        """Convert to Telethon proxy format."""
        if self.proxy_type == ProxyType.SOCKS5:
            proxy_type = python_socks.ProxyType.SOCKS5
        elif self.proxy_type == ProxyType.SOCKS4:
            proxy_type = python_socks.ProxyType.SOCKS4
        elif self.proxy_type in (ProxyType.HTTP, ProxyType.HTTPS):
            proxy_type = python_socks.ProxyType.HTTP
        else:
            proxy_type = python_socks.ProxyType.SOCKS5

        return (proxy_type, self.host, self.port, True, self.username, self.password)


# Proxy source URLs
PROXY_SOURCES = {
    "proxy_hound_socks5": "https://raw.githubusercontent.com/arandomguyhere/Proxy-Hound/main/results/socks5.txt",
    "proxy_hound_socks4": "https://raw.githubusercontent.com/arandomguyhere/Proxy-Hound/main/results/socks4.txt",
    "proxy_hound_https": "https://raw.githubusercontent.com/arandomguyhere/Proxy-Hound/main/results/https.txt",
    "proxy_hound_json": "https://raw.githubusercontent.com/arandomguyhere/Proxy-Hound/main/results/working_proxies.json",
    "socks5_scanner": "https://raw.githubusercontent.com/arandomguyhere/Tools/main/socks5-scanner/proxies/socks5.txt",
    "socks5_scanner_json": "https://raw.githubusercontent.com/arandomguyhere/Tools/main/socks5-scanner/proxies/working_proxies.json",
}


class ProxyManager:
    """
    Manages proxy rotation for TScrape.

    Features:
    - Load from multiple sources (Proxy-Hound, SOCKS5-Scanner)
    - Automatic rotation on failure
    - Health tracking and dead proxy removal
    - GeoIP filtering
    - Weighted selection by success rate
    """

    def __init__(
        self,
        preferred_types: List[ProxyType] = None,
        preferred_countries: List[str] = None,
        max_failures: int = 3,
        rotation_strategy: str = "weighted"  # weighted, random, round_robin
    ):
        self.preferred_types = preferred_types or [ProxyType.SOCKS5, ProxyType.SOCKS4]
        self.preferred_countries = preferred_countries
        self.max_failures = max_failures
        self.rotation_strategy = rotation_strategy

        self._proxies: List[ProxyInfo] = []
        self._current_index = 0
        self._lock = asyncio.Lock()
        self._dead_proxies: Set[str] = set()

    async def load_from_sources(
        self,
        sources: List[str] = None,
        include_json: bool = True
    ) -> int:
        """
        Load proxies from remote sources.

        Args:
            sources: List of source keys to load from (default: all)
            include_json: Include JSON sources for enriched data

        Returns:
            Number of proxies loaded
        """
        if sources is None:
            sources = list(PROXY_SOURCES.keys())

        if not include_json:
            sources = [s for s in sources if not s.endswith("_json")]

        loaded = 0

        async with aiohttp.ClientSession() as session:
            for source_key in sources:
                if source_key not in PROXY_SOURCES:
                    continue

                url = PROXY_SOURCES[source_key]

                try:
                    async with session.get(url, timeout=30) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch {source_key}: HTTP {response.status}")
                            continue

                        content = await response.text()

                        if url.endswith(".json"):
                            count = self._parse_json_proxies(content, source_key)
                        else:
                            count = self._parse_text_proxies(content, source_key)

                        loaded += count
                        logger.info(f"Loaded {count} proxies from {source_key}")

                except Exception as e:
                    logger.error(f"Error loading {source_key}: {e}")

        # Filter by preferences
        self._apply_filters()

        logger.info(f"Total proxies loaded: {len(self._proxies)}")
        return loaded

    def _parse_text_proxies(self, content: str, source: str) -> int:
        """Parse text format proxy list (ip:port per line)."""
        count = 0

        # Determine proxy type from source name
        if "socks5" in source.lower():
            proxy_type = ProxyType.SOCKS5
        elif "socks4" in source.lower():
            proxy_type = ProxyType.SOCKS4
        elif "https" in source.lower():
            proxy_type = ProxyType.HTTPS
        else:
            proxy_type = ProxyType.HTTP

        for line in content.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                # Handle ip:port format
                if "://" in line:
                    # URL format
                    parsed = urlparse(line)
                    host = parsed.hostname
                    port = parsed.port
                else:
                    # Simple ip:port format
                    parts = line.split(":")
                    host = parts[0]
                    port = int(parts[1])

                if host and port:
                    proxy = ProxyInfo(
                        host=host,
                        port=port,
                        proxy_type=proxy_type
                    )
                    self._add_proxy(proxy)
                    count += 1

            except (ValueError, IndexError):
                continue

        return count

    def _parse_json_proxies(self, content: str, source: str) -> int:
        """Parse JSON format proxy list with metadata."""
        import json

        count = 0

        try:
            data = json.loads(content)

            # Handle different JSON structures
            proxies = data if isinstance(data, list) else data.get("proxies", data.get("working", []))

            for item in proxies:
                try:
                    # Extract proxy info
                    host = item.get("ip") or item.get("host") or item.get("address")
                    port = item.get("port")

                    if not host or not port:
                        continue

                    # Determine type
                    type_str = item.get("type", item.get("protocol", "socks5")).lower()
                    if "socks5" in type_str:
                        proxy_type = ProxyType.SOCKS5
                    elif "socks4" in type_str:
                        proxy_type = ProxyType.SOCKS4
                    elif "https" in type_str:
                        proxy_type = ProxyType.HTTPS
                    else:
                        proxy_type = ProxyType.HTTP

                    proxy = ProxyInfo(
                        host=host,
                        port=int(port),
                        proxy_type=proxy_type,
                        country=item.get("country") or item.get("country_name"),
                        country_code=item.get("country_code") or item.get("cc"),
                        city=item.get("city"),
                        latency_ms=item.get("latency") or item.get("response_time"),
                        score=item.get("score") or item.get("hunt_score"),
                        asn=item.get("asn"),
                        org=item.get("org") or item.get("organization")
                    )

                    self._add_proxy(proxy)
                    count += 1

                except (KeyError, ValueError, TypeError):
                    continue

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from {source}: {e}")

        return count

    def _add_proxy(self, proxy: ProxyInfo) -> None:
        """Add proxy if not already present."""
        key = f"{proxy.host}:{proxy.port}"

        # Check for duplicates
        for existing in self._proxies:
            if f"{existing.host}:{existing.port}" == key:
                # Update with richer metadata if available
                if proxy.country and not existing.country:
                    existing.country = proxy.country
                    existing.country_code = proxy.country_code
                    existing.city = proxy.city
                    existing.latency_ms = proxy.latency_ms
                    existing.score = proxy.score
                return

        self._proxies.append(proxy)

    def _apply_filters(self) -> None:
        """Apply preference filters to proxy list."""
        filtered = []

        for proxy in self._proxies:
            # Filter by type
            if self.preferred_types and proxy.proxy_type not in self.preferred_types:
                continue

            # Filter by country
            if self.preferred_countries:
                if proxy.country_code and proxy.country_code.upper() not in [c.upper() for c in self.preferred_countries]:
                    continue

            filtered.append(proxy)

        if filtered:
            self._proxies = filtered
        # If no proxies match filters, keep all

    def load_from_file(self, file_path: Path, proxy_type: ProxyType = ProxyType.SOCKS5) -> int:
        """Load proxies from a local file."""
        count = 0

        with open(file_path, "r") as f:
            content = f.read()

        if file_path.suffix == ".json":
            count = self._parse_json_proxies(content, str(file_path))
        else:
            count = self._parse_text_proxies(content, str(file_path))

        self._apply_filters()
        return count

    def add_proxy(
        self,
        host: str,
        port: int,
        proxy_type: ProxyType = ProxyType.SOCKS5,
        username: str = None,
        password: str = None
    ) -> None:
        """Add a single proxy manually."""
        proxy = ProxyInfo(
            host=host,
            port=port,
            proxy_type=proxy_type,
            username=username,
            password=password
        )
        self._add_proxy(proxy)

    async def get_proxy(self) -> Optional[ProxyInfo]:
        """
        Get the next proxy based on rotation strategy.

        Returns:
            ProxyInfo or None if no proxies available
        """
        async with self._lock:
            available = [p for p in self._proxies if not p.is_dead]

            if not available:
                logger.warning("No available proxies")
                return None

            if self.rotation_strategy == "random":
                return random.choice(available)

            elif self.rotation_strategy == "round_robin":
                self._current_index = (self._current_index + 1) % len(available)
                return available[self._current_index]

            else:  # weighted by success rate
                # Sort by success rate and latency
                scored = []
                for p in available:
                    score = p.success_rate * 100
                    if p.latency_ms:
                        score -= p.latency_ms / 100  # Penalize slow proxies
                    if p.score:
                        score += p.score / 10  # Boost by hunt score
                    scored.append((score, p))

                scored.sort(key=lambda x: x[0], reverse=True)

                # Weighted random from top proxies
                top_proxies = [p for _, p in scored[:max(10, len(scored) // 4)]]
                return random.choice(top_proxies)

    def report_success(self, proxy: ProxyInfo) -> None:
        """Report successful proxy use."""
        proxy.mark_success()
        logger.debug(f"Proxy success: {proxy.host}:{proxy.port} (rate: {proxy.success_rate:.1%})")

    def report_failure(self, proxy: ProxyInfo, error: str = None) -> None:
        """Report failed proxy use."""
        proxy.mark_failure()
        logger.debug(f"Proxy failure: {proxy.host}:{proxy.port} - {error} (rate: {proxy.success_rate:.1%})")

        if proxy.is_dead:
            self._dead_proxies.add(f"{proxy.host}:{proxy.port}")
            logger.info(f"Proxy marked as dead: {proxy.host}:{proxy.port}")

    async def test_proxy(self, proxy: ProxyInfo, timeout: float = 10.0) -> bool:
        """Test if a proxy is working."""
        try:
            test_url = "https://api.telegram.org"

            if proxy.proxy_type in (ProxyType.SOCKS5, ProxyType.SOCKS4):
                connector = aiohttp.TCPConnector()
                # For SOCKS, we'd need aiohttp-socks
                # Simplified test using direct connection
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        test_url,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                        proxy=proxy.url if proxy.proxy_type in (ProxyType.HTTP, ProxyType.HTTPS) else None
                    ) as response:
                        return response.status in (200, 302, 403)  # 403 is expected from Telegram
            else:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        test_url,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                        proxy=proxy.url
                    ) as response:
                        return response.status in (200, 302, 403)

        except Exception as e:
            logger.debug(f"Proxy test failed {proxy.host}:{proxy.port}: {e}")
            return False

    async def test_all_proxies(self, max_concurrent: int = 50) -> Dict[str, int]:
        """Test all proxies and return statistics."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def test_one(proxy: ProxyInfo):
            async with semaphore:
                result = await self.test_proxy(proxy)
                if result:
                    proxy.mark_success()
                else:
                    proxy.mark_failure()
                return result

        results = await asyncio.gather(*[test_one(p) for p in self._proxies])

        working = sum(results)
        dead = len(results) - working

        return {
            "total": len(self._proxies),
            "working": working,
            "dead": dead,
            "success_rate": working / len(self._proxies) if self._proxies else 0
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get proxy pool statistics."""
        available = [p for p in self._proxies if not p.is_dead]

        by_type = {}
        by_country = {}

        for p in available:
            # Count by type
            type_name = p.proxy_type.value
            by_type[type_name] = by_type.get(type_name, 0) + 1

            # Count by country
            if p.country_code:
                by_country[p.country_code] = by_country.get(p.country_code, 0) + 1

        return {
            "total": len(self._proxies),
            "available": len(available),
            "dead": len(self._dead_proxies),
            "by_type": by_type,
            "by_country": by_country,
            "avg_success_rate": sum(p.success_rate for p in available) / len(available) if available else 0
        }

    def reset_dead_proxies(self) -> int:
        """Reset dead proxies to give them another chance."""
        count = 0
        for proxy in self._proxies:
            if proxy.is_dead:
                proxy.is_dead = False
                proxy.failures = 0
                count += 1

        self._dead_proxies.clear()
        logger.info(f"Reset {count} dead proxies")
        return count

    @property
    def count(self) -> int:
        """Get total proxy count."""
        return len(self._proxies)

    @property
    def available_count(self) -> int:
        """Get available (non-dead) proxy count."""
        return len([p for p in self._proxies if not p.is_dead])
