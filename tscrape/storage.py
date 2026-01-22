"""
Storage Manager for TScrape.

Supports multiple storage backends:
- Apache Parquet: Primary format for analytics (efficient, columnar)
- SQLite: State management and checkpointing
- JSON/CSV: Export formats for interchange
"""

import json
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from .models import ScrapedMessage, ChannelInfo, ScrapeState
from .bias import BiasTracker, BiasMetrics

logger = logging.getLogger(__name__)


# PyArrow schema for messages (optimized for analytics)
MESSAGE_SCHEMA = pa.schema([
    ('message_id', pa.int64()),
    ('channel_id', pa.int64()),
    ('channel_name', pa.string()),
    ('date', pa.timestamp('us', tz='UTC')),
    ('text', pa.string()),
    ('raw_text', pa.string()),
    ('sender_id', pa.int64()),
    ('sender_username', pa.string()),
    ('views', pa.int64()),
    ('forwards', pa.int64()),
    ('replies_count', pa.int64()),
    ('reactions_json', pa.string()),
    ('reply_to_id', pa.int64()),
    ('media_type', pa.string()),
    ('has_media', pa.bool_()),
    ('is_pinned', pa.bool_()),
    ('edit_date', pa.timestamp('us', tz='UTC')),
    ('grouped_id', pa.int64()),
    ('scraped_at', pa.timestamp('us', tz='UTC')),
])


class StorageManager:
    """
    Manages data storage with multiple backends.

    Architecture:
    - SQLite: State tracking, checkpoints, metadata
    - Parquet: Message data (batched writes for efficiency)
    - JSON/CSV: Export formats
    """

    def __init__(self, data_dir: Path, enable_bias_tracking: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # SQLite for state management
        self.db_path = self.data_dir / "tscrape_state.db"
        self._init_database()

        # In-memory buffer for batch writes
        self._write_buffer: Dict[int, List[Dict]] = {}
        self._buffer_size = 1000

        # Bias tracking for academic-grade data quality
        self._bias_tracking_enabled = enable_bias_tracking
        if enable_bias_tracking:
            self.bias_tracker = BiasTracker(self.db_path)
        else:
            self.bias_tracker = None

    def _init_database(self) -> None:
        """Initialize SQLite database for state tracking."""
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS scrape_state (
                    channel_id INTEGER PRIMARY KEY,
                    channel_name TEXT NOT NULL,
                    last_message_id INTEGER DEFAULT 0,
                    oldest_message_id INTEGER DEFAULT 0,
                    messages_scraped INTEGER DEFAULT 0,
                    media_downloaded INTEGER DEFAULT 0,
                    started_at TEXT,
                    updated_at TEXT,
                    completed INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    title TEXT NOT NULL,
                    about TEXT,
                    participants_count INTEGER,
                    is_megagroup INTEGER DEFAULT 0,
                    is_broadcast INTEGER DEFAULT 0,
                    created_at TEXT,
                    scraped_at TEXT
                );

                CREATE TABLE IF NOT EXISTS media_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    media_type TEXT,
                    file_size INTEGER DEFAULT 0,
                    mime_type TEXT,
                    file_name TEXT,
                    downloaded_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_media_channel ON media_files(channel_id);
                CREATE INDEX IF NOT EXISTS idx_media_message ON media_files(message_id);
            """)

    @contextmanager
    def _get_connection(self):
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_channel(self, channel_id: int, channel_name: str) -> None:
        """Initialize storage for a channel."""
        # Create channel directory
        channel_dir = self.data_dir / channel_name
        channel_dir.mkdir(parents=True, exist_ok=True)
        (channel_dir / "media").mkdir(exist_ok=True)

        # Initialize buffer
        self._write_buffer[channel_id] = []

        # Create or update state
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO scrape_state (channel_id, channel_name, started_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    channel_name = excluded.channel_name,
                    updated_at = excluded.updated_at
            """, (channel_id, channel_name, datetime.now(timezone.utc).isoformat(),
                  datetime.now(timezone.utc).isoformat()))

    def get_scrape_state(self, channel_id: int) -> Optional[ScrapeState]:
        """Get the current scrape state for a channel."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM scrape_state WHERE channel_id = ?",
                (channel_id,)
            ).fetchone()

            if row:
                return ScrapeState(
                    channel_id=row['channel_id'],
                    channel_name=row['channel_name'],
                    last_message_id=row['last_message_id'],
                    oldest_message_id=row['oldest_message_id'],
                    messages_scraped=row['messages_scraped'],
                    media_downloaded=row['media_downloaded'],
                    started_at=datetime.fromisoformat(row['started_at']) if row['started_at'] else None,
                    updated_at=datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None,
                    completed=bool(row['completed'])
                )
            return None

    def update_scrape_state(
        self,
        channel_id: int,
        last_message_id: Optional[int] = None,
        messages_scraped: Optional[int] = None,
        media_downloaded: Optional[int] = None,
        completed: Optional[bool] = None
    ) -> None:
        """Update scrape state (checkpoint)."""
        updates = ["updated_at = ?"]
        values = [datetime.now(timezone.utc).isoformat()]

        if last_message_id is not None:
            updates.append("last_message_id = ?")
            values.append(last_message_id)

        if messages_scraped is not None:
            updates.append("messages_scraped = ?")
            values.append(messages_scraped)

        if media_downloaded is not None:
            updates.append("media_downloaded = ?")
            values.append(media_downloaded)

        if completed is not None:
            updates.append("completed = ?")
            values.append(int(completed))

        values.append(channel_id)

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE scrape_state SET {', '.join(updates)} WHERE channel_id = ?",
                values
            )

    def save_messages(self, channel_id: int, messages: List[ScrapedMessage]) -> None:
        """Save messages to Parquet storage."""
        if not messages:
            return

        # Get channel name from first message
        channel_name = messages[0].channel_name
        channel_dir = self.data_dir / channel_name

        # Convert to flat dictionaries
        records = [m.to_flat_dict() for m in messages]

        # Add to buffer
        if channel_id not in self._write_buffer:
            self._write_buffer[channel_id] = []
        self._write_buffer[channel_id].extend(records)

        # Flush if buffer is large enough
        if len(self._write_buffer[channel_id]) >= self._buffer_size:
            self._flush_buffer(channel_id, channel_dir)

    def _flush_buffer(self, channel_id: int, channel_dir: Path) -> None:
        """Flush buffer to Parquet file."""
        if channel_id not in self._write_buffer or not self._write_buffer[channel_id]:
            return

        records = self._write_buffer[channel_id]
        self._write_buffer[channel_id] = []

        # Create DataFrame
        df = pd.DataFrame(records)

        # Handle datetime columns
        for col in ['date', 'edit_date', 'scraped_at']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)

        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_path = channel_dir / f"messages_{timestamp}.parquet"

        # Write to Parquet
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, parquet_path, compression='snappy')

        logger.debug(f"Flushed {len(records)} messages to {parquet_path}")

    def flush_all(self) -> None:
        """Flush all buffers to disk."""
        for channel_id in list(self._write_buffer.keys()):
            state = self.get_scrape_state(channel_id)
            if state:
                channel_dir = self.data_dir / state.channel_name
                self._flush_buffer(channel_id, channel_dir)

    def save_channel_info(self, info: ChannelInfo) -> None:
        """Save channel information."""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO channels (id, username, title, about, participants_count,
                    is_megagroup, is_broadcast, created_at, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    username = excluded.username,
                    title = excluded.title,
                    about = excluded.about,
                    participants_count = excluded.participants_count,
                    scraped_at = excluded.scraped_at
            """, (
                info.id, info.username, info.title, info.about,
                info.participants_count, int(info.is_megagroup),
                int(info.is_broadcast),
                info.created_at.isoformat() if info.created_at else None,
                info.scraped_at.isoformat()
            ))

    def load_messages(self, channel_name: str) -> pd.DataFrame:
        """Load all messages for a channel from Parquet files."""
        channel_dir = self.data_dir / channel_name
        parquet_files = list(channel_dir.glob("messages_*.parquet"))

        if not parquet_files:
            return pd.DataFrame()

        # Read and concatenate all Parquet files
        dfs = [pq.read_table(f).to_pandas() for f in parquet_files]
        df = pd.concat(dfs, ignore_index=True)

        # Remove duplicates (by message_id)
        df = df.drop_duplicates(subset=['message_id'], keep='last')

        return df.sort_values('date', ascending=False)

    def export_json(self, channel_name: str, output_path: Optional[Path] = None) -> Path:
        """Export channel messages to JSON."""
        df = self.load_messages(channel_name)

        if output_path is None:
            output_path = self.data_dir / channel_name / f"{channel_name}_export.json"

        # Convert to records and handle datetime
        records = df.to_dict(orient='records')
        for record in records:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logger.info(f"Exported {len(records)} messages to {output_path}")
        return output_path

    def export_csv(self, channel_name: str, output_path: Optional[Path] = None) -> Path:
        """Export channel messages to CSV."""
        df = self.load_messages(channel_name)

        if output_path is None:
            output_path = self.data_dir / channel_name / f"{channel_name}_export.csv"

        df.to_csv(output_path, index=False, encoding='utf-8')

        logger.info(f"Exported {len(df)} messages to {output_path}")
        return output_path

    def export_parquet(self, channel_name: str, output_path: Optional[Path] = None) -> Path:
        """Export channel messages to a single consolidated Parquet file."""
        df = self.load_messages(channel_name)

        if output_path is None:
            output_path = self.data_dir / channel_name / f"{channel_name}_export.parquet"

        df.to_parquet(output_path, compression='snappy', index=False)

        logger.info(f"Exported {len(df)} messages to {output_path}")
        return output_path

    def get_stats(self, channel_name: str) -> Dict[str, Any]:
        """Get statistics for a channel."""
        df = self.load_messages(channel_name)

        if df.empty:
            return {"messages": 0}

        return {
            "messages": len(df),
            "unique_senders": df['sender_id'].nunique(),
            "total_views": df['views'].sum(),
            "total_forwards": df['forwards'].sum(),
            "date_range": {
                "oldest": df['date'].min().isoformat() if not df.empty else None,
                "newest": df['date'].max().isoformat() if not df.empty else None
            },
            "media_count": df['has_media'].sum(),
            "pinned_count": df['is_pinned'].sum()
        }

    # ========== Bias Tracking Methods ==========

    def get_bias_metrics(self, channel_id: int, channel_name: str) -> Optional[BiasMetrics]:
        """
        Get bias metrics for a channel.

        Returns computed metrics including gap ratio, deletion rate, etc.
        """
        if not self.bias_tracker:
            return None
        return self.bias_tracker.compute_metrics(channel_id, channel_name)

    def get_methodology_statement(self, channel_id: int, channel_name: str) -> Optional[str]:
        """
        Generate a methodology statement for academic papers.

        Returns a formatted statement suitable for inclusion in research publications.
        """
        metrics = self.get_bias_metrics(channel_id, channel_name)
        if metrics:
            return metrics.get_methodology_statement()
        return None

    def export_bias_report(
        self,
        channel_id: int,
        channel_name: str,
        output_path: Optional[Path] = None
    ) -> Optional[Path]:
        """
        Export a comprehensive bias report for a channel.

        Args:
            channel_id: Channel identifier
            channel_name: Channel name
            output_path: Output file path (default: data_dir/channel_name/bias_report.json)

        Returns:
            Path to generated report, or None if bias tracking disabled
        """
        if not self.bias_tracker:
            return None

        if output_path is None:
            output_path = self.data_dir / channel_name / "bias_report.json"

        return self.bias_tracker.export_bias_report(channel_id, channel_name, output_path)

    def get_scrape_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent scrape run history with manifests.

        Returns list of run manifests for reproducibility tracking.
        """
        if not self.bias_tracker:
            return []
        return [r.to_dict() for r in self.bias_tracker.get_run_history(limit)]
