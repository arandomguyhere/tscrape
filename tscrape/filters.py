"""
Message Filtering Module for TScrape.

Implements keyword and pattern-based filtering as recommended in:
- arXiv:2412.16786: TelegramScrap (keyword filtering)
- arXiv:2509.20943: CTI Dataset Construction (content classification)

Features:
- Keyword matching (case-insensitive, regex support)
- Multi-language support
- Date range filtering
- Engagement thresholds (views, reactions)
- Media type filtering
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Set, Pattern, Union, Callable
from enum import Enum

from .models import ScrapedMessage

logger = logging.getLogger(__name__)


class FilterMode(Enum):
    """How to combine multiple filters."""
    ALL = "all"  # All filters must match (AND)
    ANY = "any"  # Any filter can match (OR)


@dataclass
class FilterResult:
    """Result of applying filters to a message."""
    matched: bool
    matched_filters: List[str] = field(default_factory=list)
    matched_keywords: List[str] = field(default_factory=list)


class MessageFilter:
    """
    Filters messages based on various criteria.

    Based on TelegramScrap paper's keyword filtering approach.
    """

    def __init__(
        self,
        keywords: List[str] = None,
        keywords_regex: List[str] = None,
        exclude_keywords: List[str] = None,
        min_date: datetime = None,
        max_date: datetime = None,
        min_views: int = None,
        min_reactions: int = None,
        min_forwards: int = None,
        has_media: bool = None,
        media_types: List[str] = None,
        min_text_length: int = None,
        max_text_length: int = None,
        mode: FilterMode = FilterMode.ALL,
        case_sensitive: bool = False
    ):
        """
        Initialize message filter.

        Args:
            keywords: List of keywords to match (substring)
            keywords_regex: List of regex patterns to match
            exclude_keywords: Keywords that should NOT appear
            min_date: Only messages after this date
            max_date: Only messages before this date
            min_views: Minimum view count
            min_reactions: Minimum total reactions
            min_forwards: Minimum forward count
            has_media: Filter by media presence
            media_types: Specific media types (photo, video, document)
            min_text_length: Minimum text length
            max_text_length: Maximum text length
            mode: How to combine filters (ALL/ANY)
            case_sensitive: Whether keyword matching is case-sensitive
        """
        self.mode = mode
        self.case_sensitive = case_sensitive

        # Compile keyword patterns
        self._keyword_patterns: List[Pattern] = []
        if keywords:
            for kw in keywords:
                flags = 0 if case_sensitive else re.IGNORECASE
                pattern = re.compile(re.escape(kw), flags)
                self._keyword_patterns.append(pattern)

        # Compile regex patterns
        self._regex_patterns: List[Pattern] = []
        if keywords_regex:
            for regex in keywords_regex:
                flags = 0 if case_sensitive else re.IGNORECASE
                try:
                    pattern = re.compile(regex, flags)
                    self._regex_patterns.append(pattern)
                except re.error as e:
                    logger.warning(f"Invalid regex pattern '{regex}': {e}")

        # Compile exclude patterns
        self._exclude_patterns: List[Pattern] = []
        if exclude_keywords:
            for kw in exclude_keywords:
                flags = 0 if case_sensitive else re.IGNORECASE
                pattern = re.compile(re.escape(kw), flags)
                self._exclude_patterns.append(pattern)

        # Other filters
        self.min_date = min_date
        self.max_date = max_date
        self.min_views = min_views
        self.min_reactions = min_reactions
        self.min_forwards = min_forwards
        self.has_media = has_media
        self.media_types = set(media_types) if media_types else None
        self.min_text_length = min_text_length
        self.max_text_length = max_text_length

        # Stats
        self._total_checked = 0
        self._total_matched = 0

    def matches(self, message: ScrapedMessage) -> FilterResult:
        """
        Check if a message matches the filter criteria.

        Args:
            message: Message to check

        Returns:
            FilterResult with match status and details
        """
        self._total_checked += 1

        results = []
        matched_filters = []
        matched_keywords = []

        text = message.text or ""

        # Check exclude keywords first (always AND logic)
        for pattern in self._exclude_patterns:
            if pattern.search(text):
                return FilterResult(matched=False, matched_filters=["excluded"])

        # Keyword matching
        if self._keyword_patterns:
            keyword_matches = []
            for pattern in self._keyword_patterns:
                match = pattern.search(text)
                if match:
                    keyword_matches.append(match.group())

            if keyword_matches:
                results.append(True)
                matched_filters.append("keywords")
                matched_keywords.extend(keyword_matches)
            else:
                results.append(False)

        # Regex matching
        if self._regex_patterns:
            regex_matches = []
            for pattern in self._regex_patterns:
                match = pattern.search(text)
                if match:
                    regex_matches.append(match.group())

            if regex_matches:
                results.append(True)
                matched_filters.append("regex")
                matched_keywords.extend(regex_matches)
            else:
                results.append(False)

        # Date filtering
        if self.min_date:
            if message.date and message.date >= self.min_date:
                results.append(True)
                matched_filters.append("min_date")
            else:
                results.append(False)

        if self.max_date:
            if message.date and message.date <= self.max_date:
                results.append(True)
                matched_filters.append("max_date")
            else:
                results.append(False)

        # View count
        if self.min_views is not None:
            if message.views >= self.min_views:
                results.append(True)
                matched_filters.append("min_views")
            else:
                results.append(False)

        # Reaction count
        if self.min_reactions is not None:
            total_reactions = sum(r.get('count', 0) for r in message.reactions)
            if total_reactions >= self.min_reactions:
                results.append(True)
                matched_filters.append("min_reactions")
            else:
                results.append(False)

        # Forward count
        if self.min_forwards is not None:
            if message.forwards >= self.min_forwards:
                results.append(True)
                matched_filters.append("min_forwards")
            else:
                results.append(False)

        # Media presence
        if self.has_media is not None:
            if message.has_media == self.has_media:
                results.append(True)
                matched_filters.append("has_media")
            else:
                results.append(False)

        # Media type
        if self.media_types and message.media_type:
            media_type_lower = message.media_type.lower()
            if any(mt.lower() in media_type_lower for mt in self.media_types):
                results.append(True)
                matched_filters.append("media_type")
            else:
                results.append(False)

        # Text length
        if self.min_text_length is not None:
            if len(text) >= self.min_text_length:
                results.append(True)
                matched_filters.append("min_text_length")
            else:
                results.append(False)

        if self.max_text_length is not None:
            if len(text) <= self.max_text_length:
                results.append(True)
                matched_filters.append("max_text_length")
            else:
                results.append(False)

        # Combine results
        if not results:
            # No filters applied, match everything
            matched = True
        elif self.mode == FilterMode.ALL:
            matched = all(results)
        else:  # ANY
            matched = any(results)

        if matched:
            self._total_matched += 1

        return FilterResult(
            matched=matched,
            matched_filters=matched_filters if matched else [],
            matched_keywords=matched_keywords if matched else []
        )

    def filter_messages(
        self,
        messages: List[ScrapedMessage]
    ) -> List[ScrapedMessage]:
        """
        Filter a list of messages.

        Args:
            messages: List of messages to filter

        Returns:
            List of matching messages
        """
        return [msg for msg in messages if self.matches(msg).matched]

    def get_stats(self) -> dict:
        """Get filter statistics."""
        return {
            "total_checked": self._total_checked,
            "total_matched": self._total_matched,
            "match_rate": self._total_matched / self._total_checked if self._total_checked > 0 else 0
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._total_checked = 0
        self._total_matched = 0


class KeywordSet:
    """
    Pre-defined keyword sets for common use cases.

    Based on CTI paper's threat intelligence categories.
    """

    # Cyber threat intelligence keywords
    CTI_KEYWORDS = [
        # Malware
        "malware", "ransomware", "trojan", "backdoor", "rootkit",
        "botnet", "worm", "spyware", "keylogger", "rat",
        # Attacks
        "ddos", "phishing", "exploit", "vulnerability", "cve-",
        "zero-day", "0day", "breach", "hack", "attack",
        # Indicators
        "ioc", "indicator", "hash", "md5", "sha256",
        "ip address", "domain", "url", "c2", "c&c",
        # Tools
        "cobalt strike", "metasploit", "mimikatz", "powershell",
    ]

    # Cryptocurrency keywords
    CRYPTO_KEYWORDS = [
        "bitcoin", "btc", "ethereum", "eth", "crypto",
        "wallet", "blockchain", "defi", "nft", "token",
        "exchange", "trading", "pump", "dump", "moon",
    ]

    # News/Media keywords
    NEWS_KEYWORDS = [
        "breaking", "urgent", "update", "official",
        "announcement", "confirmed", "reported",
    ]

    @classmethod
    def get_cti_filter(cls) -> MessageFilter:
        """Get a filter for cyber threat intelligence content."""
        return MessageFilter(
            keywords=cls.CTI_KEYWORDS,
            case_sensitive=False,
            mode=FilterMode.ANY
        )

    @classmethod
    def get_crypto_filter(cls) -> MessageFilter:
        """Get a filter for cryptocurrency content."""
        return MessageFilter(
            keywords=cls.CRYPTO_KEYWORDS,
            case_sensitive=False,
            mode=FilterMode.ANY
        )

    @classmethod
    def get_viral_filter(cls, min_views: int = 10000) -> MessageFilter:
        """Get a filter for viral/popular content."""
        return MessageFilter(
            min_views=min_views,
            mode=FilterMode.ALL
        )


def create_filter_from_file(file_path: str) -> MessageFilter:
    """
    Create a filter from a keywords file (one keyword per line).

    Args:
        file_path: Path to keywords file

    Returns:
        MessageFilter configured with keywords
    """
    keywords = []
    regex_patterns = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Check if it's a regex pattern (starts with /)
            if line.startswith('/') and line.endswith('/'):
                regex_patterns.append(line[1:-1])
            else:
                keywords.append(line)

    return MessageFilter(
        keywords=keywords if keywords else None,
        keywords_regex=regex_patterns if regex_patterns else None,
        mode=FilterMode.ANY
    )
