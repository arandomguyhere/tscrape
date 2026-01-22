"""
Bias Tracking Module for TScrape.

Implements academic-grade data quality and bias measurement following
computational social science and OSINT research standards.

Features:
- Message continuity tracking (gap detection)
- Scrape run manifests (reproducibility)
- Deletion/edit awareness
- Dataset-level bias metrics

Based on methodology from:
- Computational social science best practices
- OSINT research standards
- Digital trace data collection guidelines
"""

import json
import hashlib
import logging
import platform
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from enum import Enum
import uuid

logger = logging.getLogger(__name__)


class MessageStatus(Enum):
    """Status of a message in continuity tracking."""
    OBSERVED = "observed"      # Message was successfully collected
    DELETED = "deleted"        # Message confirmed deleted (API returned empty)
    INACCESSIBLE = "inaccessible"  # Message exists but cannot be accessed
    UNKNOWN = "unknown"        # Gap detected, status not yet determined
    EDITED = "edited"          # Message was modified after initial collection


@dataclass
class MessageContinuity:
    """Tracks expected vs observed message IDs for gap detection."""
    channel_id: int
    expected_msg_id: int
    observed: bool
    first_seen_ts: Optional[datetime] = None
    last_checked_ts: Optional[datetime] = None
    status: MessageStatus = MessageStatus.UNKNOWN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "expected_msg_id": self.expected_msg_id,
            "observed": self.observed,
            "first_seen_ts": self.first_seen_ts.isoformat() if self.first_seen_ts else None,
            "last_checked_ts": self.last_checked_ts.isoformat() if self.last_checked_ts else None,
            "status": self.status.value
        }


@dataclass
class MessageStatusHistory:
    """Tracks changes to a message over time (edits, deletions)."""
    channel_id: int
    message_id: int
    observed_ts: datetime
    status: MessageStatus
    text_checksum: Optional[str] = None  # SHA256 of text content
    text_length: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "message_id": self.message_id,
            "observed_ts": self.observed_ts.isoformat(),
            "status": self.status.value,
            "text_checksum": self.text_checksum,
            "text_length": self.text_length
        }


@dataclass
class ScrapeRunManifest:
    """
    Run-level manifest for reproducibility.

    Enables independent replication and validation of data collection.
    """
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tool_version: str = ""
    telethon_version: str = ""
    python_version: str = field(default_factory=lambda: platform.python_version())
    platform_info: str = field(default_factory=lambda: f"{platform.system()} {platform.release()}")
    start_time_utc: Optional[datetime] = None
    end_time_utc: Optional[datetime] = None
    channels: List[str] = field(default_factory=list)
    scrape_mode: str = "incremental"  # incremental | full | sample
    sampling_interval_minutes: Optional[int] = None
    message_limit: Optional[int] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Runtime stats
    messages_collected: int = 0
    messages_skipped: int = 0
    errors_encountered: int = 0
    flood_waits: int = 0
    proxy_rotations: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "tool_version": self.tool_version,
            "telethon_version": self.telethon_version,
            "python_version": self.python_version,
            "platform_info": self.platform_info,
            "start_time_utc": self.start_time_utc.isoformat() if self.start_time_utc else None,
            "end_time_utc": self.end_time_utc.isoformat() if self.end_time_utc else None,
            "channels": self.channels,
            "scrape_mode": self.scrape_mode,
            "sampling_interval_minutes": self.sampling_interval_minutes,
            "message_limit": self.message_limit,
            "parameters": self.parameters,
            "runtime_stats": {
                "messages_collected": self.messages_collected,
                "messages_skipped": self.messages_skipped,
                "errors_encountered": self.errors_encountered,
                "flood_waits": self.flood_waits,
                "proxy_rotations": self.proxy_rotations
            }
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


@dataclass
class BiasMetrics:
    """
    Dataset-level bias metrics.

    These metrics should be reported alongside analytical results.
    """
    channel_id: int
    channel_name: str

    # Coverage metrics
    expected_message_count: int = 0      # Based on ID range
    observed_message_count: int = 0      # Actually collected
    gap_count: int = 0                   # Missing IDs in range

    # Deletion metrics
    confirmed_deleted: int = 0
    possibly_deleted: int = 0

    # Edit metrics
    edited_messages: int = 0

    # Temporal metrics
    oldest_message_ts: Optional[datetime] = None
    newest_message_ts: Optional[datetime] = None
    collection_start_ts: Optional[datetime] = None
    collection_end_ts: Optional[datetime] = None
    avg_sampling_latency_seconds: Optional[float] = None

    # Computed rates
    @property
    def gap_ratio(self) -> float:
        """Missing message IDs / expected IDs."""
        if self.expected_message_count == 0:
            return 0.0
        return self.gap_count / self.expected_message_count

    @property
    def deletion_rate(self) -> float:
        """Deleted messages / total observed."""
        if self.observed_message_count == 0:
            return 0.0
        return self.confirmed_deleted / self.observed_message_count

    @property
    def coverage_rate(self) -> float:
        """Observed / expected messages."""
        if self.expected_message_count == 0:
            return 0.0
        return self.observed_message_count / self.expected_message_count

    @property
    def edit_rate(self) -> float:
        """Edited messages / observed messages."""
        if self.observed_message_count == 0:
            return 0.0
        return self.edited_messages / self.observed_message_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "channel_name": self.channel_name,
            "coverage": {
                "expected_messages": self.expected_message_count,
                "observed_messages": self.observed_message_count,
                "gap_count": self.gap_count,
                "gap_ratio": round(self.gap_ratio, 4),
                "coverage_rate": round(self.coverage_rate, 4)
            },
            "deletions": {
                "confirmed_deleted": self.confirmed_deleted,
                "possibly_deleted": self.possibly_deleted,
                "deletion_rate": round(self.deletion_rate, 4)
            },
            "edits": {
                "edited_messages": self.edited_messages,
                "edit_rate": round(self.edit_rate, 4)
            },
            "temporal": {
                "oldest_message": self.oldest_message_ts.isoformat() if self.oldest_message_ts else None,
                "newest_message": self.newest_message_ts.isoformat() if self.newest_message_ts else None,
                "collection_start": self.collection_start_ts.isoformat() if self.collection_start_ts else None,
                "collection_end": self.collection_end_ts.isoformat() if self.collection_end_ts else None,
                "avg_sampling_latency_seconds": self.avg_sampling_latency_seconds
            }
        }

    def get_methodology_statement(self) -> str:
        """
        Generate a methodology statement for academic papers.

        Example output for inclusion in research publications.
        """
        statement = f"Data collection for channel '{self.channel_name}' "

        if self.collection_start_ts and self.collection_end_ts:
            statement += f"occurred between {self.collection_start_ts.strftime('%Y-%m-%d')} "
            statement += f"and {self.collection_end_ts.strftime('%Y-%m-%d')}. "

        statement += f"Approximately {self.gap_ratio:.1%} of message IDs within the observed range "
        statement += "were unavailable at collection time, consistent with deletion or access restrictions. "

        if self.deletion_rate > 0:
            statement += f"The confirmed deletion rate was {self.deletion_rate:.1%}. "

        if self.edit_rate > 0:
            statement += f"Approximately {self.edit_rate:.1%} of collected messages showed evidence of post-publication editing. "

        if self.avg_sampling_latency_seconds:
            hours = self.avg_sampling_latency_seconds / 3600
            statement += f"Average sampling latency was {hours:.1f} hours, "
            statement += "which may underrepresent short-lived content."

        return statement


class BiasTracker:
    """
    Tracks data collection bias for academic-grade research.

    Usage:
        tracker = BiasTracker(db_path)
        tracker.start_run(channels=["@channel1"])

        # During scraping
        tracker.record_message(channel_id, msg_id, text)
        tracker.record_gap(channel_id, missing_id)

        # After scraping
        tracker.end_run()
        metrics = tracker.compute_metrics(channel_id)
        print(metrics.get_methodology_statement())
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._current_run: Optional[ScrapeRunManifest] = None
        self._observed_ids: Dict[int, Set[int]] = {}  # channel_id -> set of msg_ids
        self._init_database()

    def _init_database(self) -> None:
        """Initialize bias tracking tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                -- Message continuity tracking
                CREATE TABLE IF NOT EXISTS message_continuity (
                    channel_id INTEGER NOT NULL,
                    expected_msg_id INTEGER NOT NULL,
                    observed INTEGER DEFAULT 0,
                    first_seen_ts TEXT,
                    last_checked_ts TEXT,
                    status TEXT DEFAULT 'unknown',
                    PRIMARY KEY (channel_id, expected_msg_id)
                );

                -- Message status history (edits, deletions)
                CREATE TABLE IF NOT EXISTS message_status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    observed_ts TEXT NOT NULL,
                    status TEXT NOT NULL,
                    text_checksum TEXT,
                    text_length INTEGER
                );

                -- Scrape run manifests
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    run_id TEXT PRIMARY KEY,
                    manifest_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                -- Indexes for performance
                CREATE INDEX IF NOT EXISTS idx_continuity_channel
                    ON message_continuity(channel_id);
                CREATE INDEX IF NOT EXISTS idx_continuity_status
                    ON message_continuity(status);
                CREATE INDEX IF NOT EXISTS idx_history_channel_msg
                    ON message_status_history(channel_id, message_id);
                CREATE INDEX IF NOT EXISTS idx_history_status
                    ON message_status_history(status);
            """)

    def start_run(
        self,
        channels: List[str],
        scrape_mode: str = "incremental",
        message_limit: Optional[int] = None,
        **parameters
    ) -> ScrapeRunManifest:
        """Start a new scrape run and create manifest."""
        # Get versions
        try:
            import telethon
            telethon_version = telethon.__version__
        except:
            telethon_version = "unknown"

        try:
            from . import __version__
            tool_version = f"tscrape {__version__}"
        except:
            tool_version = "tscrape unknown"

        self._current_run = ScrapeRunManifest(
            tool_version=tool_version,
            telethon_version=telethon_version,
            start_time_utc=datetime.now(timezone.utc),
            channels=channels,
            scrape_mode=scrape_mode,
            message_limit=message_limit,
            parameters=parameters
        )

        self._observed_ids = {}

        logger.info(f"Started scrape run {self._current_run.run_id}")
        return self._current_run

    def end_run(self) -> Optional[ScrapeRunManifest]:
        """End the current run and save manifest."""
        if not self._current_run:
            return None

        self._current_run.end_time_utc = datetime.now(timezone.utc)

        # Save manifest to database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO scrape_runs (run_id, manifest_json, created_at) VALUES (?, ?, ?)",
                (
                    self._current_run.run_id,
                    self._current_run.to_json(),
                    datetime.now(timezone.utc).isoformat()
                )
            )

        # Save manifest to file
        manifest_path = self.db_path.parent / f"manifest_{self._current_run.run_id[:8]}.json"
        with open(manifest_path, 'w') as f:
            f.write(self._current_run.to_json())

        logger.info(f"Ended scrape run {self._current_run.run_id}, manifest saved to {manifest_path}")

        run = self._current_run
        self._current_run = None
        return run

    def record_message(
        self,
        channel_id: int,
        message_id: int,
        text: Optional[str] = None,
        check_edit: bool = True
    ) -> None:
        """
        Record an observed message for continuity tracking.

        Args:
            channel_id: Channel identifier
            message_id: Message identifier
            text: Message text (for checksum calculation)
            check_edit: Whether to check for edits against previous observations
        """
        now = datetime.now(timezone.utc)

        # Track in memory
        if channel_id not in self._observed_ids:
            self._observed_ids[channel_id] = set()
        self._observed_ids[channel_id].add(message_id)

        # Calculate checksum
        checksum = None
        text_length = None
        if text:
            checksum = hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]
            text_length = len(text)

        with sqlite3.connect(self.db_path) as conn:
            # Update continuity table
            conn.execute("""
                INSERT INTO message_continuity
                    (channel_id, expected_msg_id, observed, first_seen_ts, last_checked_ts, status)
                VALUES (?, ?, 1, ?, ?, 'observed')
                ON CONFLICT(channel_id, expected_msg_id) DO UPDATE SET
                    observed = 1,
                    last_checked_ts = excluded.last_checked_ts,
                    status = 'observed'
            """, (channel_id, message_id, now.isoformat(), now.isoformat()))

            # Check for edits
            if check_edit and checksum:
                cursor = conn.execute("""
                    SELECT text_checksum FROM message_status_history
                    WHERE channel_id = ? AND message_id = ? AND status = 'observed'
                    ORDER BY observed_ts DESC LIMIT 1
                """, (channel_id, message_id))

                row = cursor.fetchone()
                if row and row[0] and row[0] != checksum:
                    # Message was edited
                    conn.execute("""
                        INSERT INTO message_status_history
                            (channel_id, message_id, observed_ts, status, text_checksum, text_length)
                        VALUES (?, ?, ?, 'edited', ?, ?)
                    """, (channel_id, message_id, now.isoformat(), checksum, text_length))

                    conn.execute("""
                        UPDATE message_continuity SET status = 'edited'
                        WHERE channel_id = ? AND expected_msg_id = ?
                    """, (channel_id, message_id))
                else:
                    # Record observation
                    conn.execute("""
                        INSERT INTO message_status_history
                            (channel_id, message_id, observed_ts, status, text_checksum, text_length)
                        VALUES (?, ?, ?, 'observed', ?, ?)
                    """, (channel_id, message_id, now.isoformat(), checksum, text_length))

        if self._current_run:
            self._current_run.messages_collected += 1

    def record_gap(
        self,
        channel_id: int,
        missing_msg_id: int,
        status: MessageStatus = MessageStatus.UNKNOWN
    ) -> None:
        """Record a gap in message continuity."""
        now = datetime.now(timezone.utc)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO message_continuity
                    (channel_id, expected_msg_id, observed, first_seen_ts, last_checked_ts, status)
                VALUES (?, ?, 0, ?, ?, ?)
                ON CONFLICT(channel_id, expected_msg_id) DO UPDATE SET
                    last_checked_ts = excluded.last_checked_ts,
                    status = CASE
                        WHEN message_continuity.status = 'observed' THEN 'deleted'
                        ELSE excluded.status
                    END
            """, (channel_id, missing_msg_id, now.isoformat(), now.isoformat(), status.value))

    def record_deletion(self, channel_id: int, message_id: int) -> None:
        """Record a confirmed message deletion."""
        now = datetime.now(timezone.utc)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE message_continuity
                SET status = 'deleted', last_checked_ts = ?
                WHERE channel_id = ? AND expected_msg_id = ?
            """, (now.isoformat(), channel_id, message_id))

            conn.execute("""
                INSERT INTO message_status_history
                    (channel_id, message_id, observed_ts, status)
                VALUES (?, ?, ?, 'deleted')
            """, (channel_id, message_id, now.isoformat()))

    def record_flood_wait(self) -> None:
        """Record a FloodWait event."""
        if self._current_run:
            self._current_run.flood_waits += 1

    def record_proxy_rotation(self) -> None:
        """Record a proxy rotation event."""
        if self._current_run:
            self._current_run.proxy_rotations += 1

    def record_error(self) -> None:
        """Record an error event."""
        if self._current_run:
            self._current_run.errors_encountered += 1

    def detect_gaps(
        self,
        channel_id: int,
        min_msg_id: int,
        max_msg_id: int
    ) -> List[int]:
        """
        Detect gaps in message ID sequence.

        Args:
            channel_id: Channel to check
            min_msg_id: Start of expected range
            max_msg_id: End of expected range

        Returns:
            List of missing message IDs
        """
        observed = self._observed_ids.get(channel_id, set())
        expected = set(range(min_msg_id, max_msg_id + 1))
        gaps = expected - observed

        # Record gaps
        for gap_id in gaps:
            self.record_gap(channel_id, gap_id)

        return sorted(gaps)

    def compute_metrics(self, channel_id: int, channel_name: str = "") -> BiasMetrics:
        """
        Compute bias metrics for a channel.

        Args:
            channel_id: Channel identifier
            channel_name: Channel name for display

        Returns:
            BiasMetrics with computed values
        """
        metrics = BiasMetrics(channel_id=channel_id, channel_name=channel_name)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Count by status
            cursor = conn.execute("""
                SELECT status, COUNT(*) as cnt
                FROM message_continuity
                WHERE channel_id = ?
                GROUP BY status
            """, (channel_id,))

            for row in cursor:
                status = row['status']
                count = row['cnt']

                if status == 'observed':
                    metrics.observed_message_count += count
                elif status == 'deleted':
                    metrics.confirmed_deleted += count
                elif status == 'edited':
                    metrics.edited_messages += count
                    metrics.observed_message_count += count
                elif status in ('unknown', 'inaccessible'):
                    metrics.possibly_deleted += count

            # Get ID range for expected count
            cursor = conn.execute("""
                SELECT MIN(expected_msg_id) as min_id, MAX(expected_msg_id) as max_id
                FROM message_continuity
                WHERE channel_id = ?
            """, (channel_id,))

            row = cursor.fetchone()
            if row and row['min_id'] and row['max_id']:
                metrics.expected_message_count = row['max_id'] - row['min_id'] + 1
                metrics.gap_count = metrics.expected_message_count - metrics.observed_message_count

            # Get temporal info from status history
            cursor = conn.execute("""
                SELECT MIN(observed_ts) as first_ts, MAX(observed_ts) as last_ts
                FROM message_status_history
                WHERE channel_id = ? AND status = 'observed'
            """, (channel_id,))

            row = cursor.fetchone()
            if row and row['first_ts']:
                metrics.collection_start_ts = datetime.fromisoformat(row['first_ts'])
                metrics.collection_end_ts = datetime.fromisoformat(row['last_ts'])

        return metrics

    def get_run_history(self, limit: int = 10) -> List[ScrapeRunManifest]:
        """Get recent scrape run manifests."""
        runs = []

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT manifest_json FROM scrape_runs
                ORDER BY created_at DESC LIMIT ?
            """, (limit,))

            for row in cursor:
                data = json.loads(row[0])

                # Parse datetime strings back to datetime objects
                if data.get('start_time_utc') and isinstance(data['start_time_utc'], str):
                    data['start_time_utc'] = datetime.fromisoformat(data['start_time_utc'].replace('Z', '+00:00'))
                if data.get('end_time_utc') and isinstance(data['end_time_utc'], str):
                    data['end_time_utc'] = datetime.fromisoformat(data['end_time_utc'].replace('Z', '+00:00'))

                run = ScrapeRunManifest(**{
                    k: v for k, v in data.items()
                    if k != 'runtime_stats'
                })
                if 'runtime_stats' in data:
                    for k, v in data['runtime_stats'].items():
                        setattr(run, k, v)
                runs.append(run)

        return runs

    def export_bias_report(
        self,
        channel_id: int,
        channel_name: str,
        output_path: Path
    ) -> Path:
        """
        Export a comprehensive bias report for a channel.

        Args:
            channel_id: Channel identifier
            channel_name: Channel name
            output_path: Output file path

        Returns:
            Path to generated report
        """
        metrics = self.compute_metrics(channel_id, channel_name)

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "channel": {
                "id": channel_id,
                "name": channel_name
            },
            "metrics": metrics.to_dict(),
            "methodology_statement": metrics.get_methodology_statement(),
            "recent_runs": [r.to_dict() for r in self.get_run_history(5)]
        }

        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Exported bias report to {output_path}")
        return output_path
