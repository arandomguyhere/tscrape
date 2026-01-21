# TScrape - Modern Telegram Channel Scraper (2026)

A high-performance Telegram scraper combining best practices from:
- [unnohwn/telegram-scraper](https://github.com/unnohwn/telegram-scraper) - Resume support, parallel media
- [ergoncugler/web-scraping-telegram](https://github.com/ergoncugler/web-scraping-telegram) - Parquet storage, analytics
- [Telethon](https://github.com/LonamiWebs/Telethon) - FloodWait handling, session management

## Features

| Feature | Description |
|---------|-------------|
| **Telethon MTProto API** | Direct API access, not web scraping |
| **Incremental Scraping** | Resume from checkpoint after interruption |
| **Parquet Storage** | Efficient columnar format for analytics |
| **Parallel Media Downloads** | Configurable concurrent downloads |
| **FloodWait Handling** | Exponential backoff, automatic retry |
| **Rich Metadata** | Views, forwards, reactions, replies |
| **Multiple Exports** | Parquet, JSON, CSV formats |
| **Progress Tracking** | Real-time progress bars |

## Installation

```bash
# Clone the repository
git clone https://github.com/your-repo/tscrape.git
cd tscrape

# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

## Quick Start

### 1. Get API Credentials

1. Visit [my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Go to "API Development Tools"
4. Create a new application
5. Note your `api_id` and `api_hash`

### 2. Set Environment Variables

```bash
export TELEGRAM_API_ID=your_api_id
export TELEGRAM_API_HASH=your_api_hash
```

### 3. Run TScrape

```bash
# Scrape a channel
tscrape scrape @channelname

# Scrape with media downloads
tscrape scrape @channelname --media --limit 1000

# List your accessible channels
tscrape channels

# Export to different formats
tscrape export channelname --format json
tscrape export channelname --format csv

# View statistics
tscrape stats channelname
```

## Usage

### Scrape a Channel

```bash
# Basic scrape (all messages)
tscrape scrape @durov

# Limit messages
tscrape scrape @durov --limit 500

# With media downloads
tscrape scrape @durov --media

# Disable resume (start fresh)
tscrape scrape @durov --no-resume
```

### Python API

```python
import asyncio
from tscrape import TelegramScraper

async def main():
    async with TelegramScraper(
        api_id=YOUR_API_ID,
        api_hash="YOUR_API_HASH",
        data_dir="./data"
    ) as scraper:

        # List channels
        channels = await scraper.get_dialogs()
        for ch in channels:
            print(f"{ch['name']} (@{ch['username']})")

        # Scrape messages
        async for msg in scraper.scrape_channel("@channelname", limit=100):
            print(f"[{msg.date}] {msg.text[:50]}...")

asyncio.run(main())
```

### Export Data

```bash
# Export to Parquet (recommended for analysis)
tscrape export channelname --format parquet

# Export to JSON
tscrape export channelname --format json

# Export to CSV
tscrape export channelname --format csv -o /path/to/output.csv
```

## Architecture

```
tscrape/
├── __init__.py       # Package exports
├── scraper.py        # Main TelegramScraper class
├── storage.py        # Parquet + SQLite storage
├── media.py          # Parallel media downloader
├── models.py         # Data models (ScrapedMessage, etc.)
├── config.py         # Configuration management
└── cli.py            # Command-line interface

data/
├── tscrape_state.db  # SQLite state (checkpoints)
└── channelname/
    ├── messages_*.parquet  # Message data
    └── media/              # Downloaded media
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TELEGRAM_API_ID` | Telegram API ID | Required |
| `TELEGRAM_API_HASH` | Telegram API Hash | Required |
| `TSCRAPE_DATA_DIR` | Data directory | `./data` |
| `TSCRAPE_BATCH_SIZE` | Messages per batch | `100` |
| `TSCRAPE_MEDIA_CONCURRENT` | Parallel downloads | `3` |
| `TSCRAPE_LOG_LEVEL` | Logging level | `INFO` |

### Config File

```bash
# Create config interactively
tscrape init

# Use config file
tscrape -c tscrape_config.json scrape @channel
```

## Best Practices

### Rate Limiting

TScrape automatically handles Telegram's rate limits:
- Exponential backoff on FloodWait
- Configurable delays between requests
- Session persistence to avoid re-authentication

### Large Scrapes

For scraping large channels (100K+ messages):

1. **Use a dedicated account** - Don't risk your main account
2. **Run on a server** - Avoid network interruptions
3. **Enable resume** - Default, automatically checkpoints
4. **Scrape in batches** - Telegram soft-bans after ~200 channels/day

### Storage Efficiency

- **Parquet**: 10-20x smaller than JSON, fastest for analytics
- **SQLite**: Only for state, not message data
- **JSON/CSV**: Export only, not primary storage

## Data Schema

### Message Fields

| Field | Type | Description |
|-------|------|-------------|
| `message_id` | int | Unique message ID |
| `channel_id` | int | Channel ID |
| `date` | datetime | Message timestamp (UTC) |
| `text` | str | Message text |
| `views` | int | View count |
| `forwards` | int | Forward count |
| `replies_count` | int | Reply count |
| `reactions` | list | Reaction emoji + counts |
| `sender_id` | int | Sender user ID |
| `media_type` | str | Photo/Video/Document/etc |
| `reply_to_id` | int | Reply thread |

## Troubleshooting

### "FloodWait" errors

Normal behavior. TScrape automatically waits and retries.

### "Channel private" error

You must be a member of the channel to scrape it.

### Session expires

Delete the session file and re-authenticate:
```bash
rm data/tscrape_session.session
```

### Import errors

```bash
# Install with crypto acceleration
pip install cryptg
```

## License

MIT License - See LICENSE file

## Acknowledgments

Built on the excellent [Telethon](https://github.com/LonamiWebs/Telethon) library.

Inspired by:
- [unnohwn/telegram-scraper](https://github.com/unnohwn/telegram-scraper)
- [ergoncugler/web-scraping-telegram](https://github.com/ergoncugler/web-scraping-telegram)
