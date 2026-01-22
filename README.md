# TScrape - Modern Telegram Channel Scraper (2026)

[![Version](https://img.shields.io/badge/version-1.4.0-blue.svg)](https://github.com/arandomguyhere/tscrape)
[![Python](https://img.shields.io/badge/python-3.9+-green.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)
[![Telethon](https://img.shields.io/badge/telethon-MTProto-purple.svg)](https://github.com/LonamiWebs/Telethon)

A high-performance Telegram scraper combining best practices from:
- [unnohwn/telegram-scraper](https://github.com/unnohwn/telegram-scraper) - Resume support, parallel media
- [ergoncugler/web-scraping-telegram](https://github.com/ergoncugler/web-scraping-telegram) - Parquet storage, analytics
- [Telethon](https://github.com/LonamiWebs/Telethon) - FloodWait handling, session management
- [Proxy-Hound](https://github.com/arandomguyhere/Proxy-Hound) & [SOCKS5-Scanner](https://github.com/arandomguyhere/Tools/tree/main/socks5-scanner) - Proxy rotation

Informed by academic research:
- [TelegramScrap (arXiv:2412.16786)](https://arxiv.org/abs/2412.16786) - Keyword filtering methodology
- [CTI Dataset Construction (arXiv:2509.20943)](https://arxiv.org/abs/2509.20943) - Content classification
- [PLOS ONE Narrative Analysis](https://journals.plos.org/plosone/) - Snowballing method for channel discovery

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Command Reference](#cli-command-reference)
- [Backends](#backends)
- [Usage](#usage)
- [Proxy Support](#proxy-support)
- [Channel Discovery](#channel-discovery-snowballing)
- [Keyword Filtering](#keyword-filtering)
- [Bias Tracking & Methodology](#bias-tracking--methodology)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Best Practices](#best-practices)
- [Data Schema](#data-schema)
- [Troubleshooting](#troubleshooting)
- [License](#license)
- [Acknowledgments](#acknowledgments)

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
| **Proxy Rotation** | Auto-load from Proxy-Hound/SOCKS5-Scanner |
| **Health Tracking** | Auto-skip dead proxies |
| **Channel Discovery** | Snowballing via forward analysis |
| **Keyword Filtering** | Regex support, preset CTI/crypto filters |
| **Network Export** | GraphML/GEXF for Gephi visualization |
| **Bias Tracking** | Academic-grade gap/deletion detection |
| **Run Manifests** | Reproducibility metadata for research |
| **Pluggable Backends** | API (telethon) or web HTML (no API needed) |

## Installation

```bash
# Clone the repository
git clone https://github.com/arandomguyhere/tscrape.git
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

# Discover related channels (snowballing)
tscrape discover snowball @seedchannel --depth 2

# Filter messages by keywords
tscrape filter channelname --preset cti -o threats.json
```

## CLI Command Reference

| Command | Description |
|---------|-------------|
| `tscrape scrape @channel` | Scrape messages from a channel |
| `tscrape scrape @channel --media` | Scrape with media downloads |
| `tscrape scrape @channel --limit N` | Limit to N messages |
| `tscrape scrape @channel --proxy` | Use proxy rotation |
| `tscrape channels` | List accessible channels |
| `tscrape export channel --format json` | Export to JSON |
| `tscrape export channel --format csv` | Export to CSV |
| `tscrape export channel --format parquet` | Export to Parquet |
| `tscrape stats channel` | View channel statistics |
| `tscrape init` | Create config interactively |
| **Discovery** | |
| `tscrape discover snowball @ch` | Discover related channels |
| `tscrape discover snowball @ch --depth 2` | Deeper discovery |
| `tscrape discover network @ch --format graphml` | Export network graph |
| **Filtering** | |
| `tscrape filter channel --keywords X` | Filter by keywords |
| `tscrape filter channel --preset cti` | Use CTI preset |
| `tscrape filter channel --regex "pattern"` | Filter by regex |
| **Proxy** | |
| `tscrape proxy load` | Load proxy pool |
| `tscrape proxy test -f file.txt` | Test proxies |
| `tscrape proxy sources` | List proxy sources |
| **Bias Tracking** | |
| `tscrape bias metrics channel` | View bias metrics |
| `tscrape bias report channel` | Export bias report |
| `tscrape bias history` | View scrape history |
| `tscrape bias statement channel` | Generate methodology statement |

## Backends

TScrape supports pluggable backends for different scraping strategies:

| Backend | Command | API Required | Use Case |
|---------|---------|--------------|----------|
| **telethon** (default) | `--backend telethon` | Yes | Full-fidelity archival research |
| **web** | `--backend web` | No | OSINT monitoring, no account needed |

### Telethon Backend (Default)

Full API access via MTProto. Reference standard for research.

```bash
# Default - uses Telethon API
tscrape scrape @channel

# Explicit
tscrape scrape @channel --backend telethon
```

**Capabilities:**
- Public and private channels
- Full message metadata (views, reactions, forwards)
- Edit and deletion detection
- Media downloads
- Resume by message ID
- Full bias tracking support

### Web HTML Backend (No API)

Scrapes public web interface at `t.me/s/channelname`. No Telegram account required.

```bash
# No API credentials needed
tscrape scrape @channel --backend web

# With limit
tscrape scrape @channel --backend web --limit 500
```

**Use cases:**
- No Telegram account available
- Account risk mitigation
- Quick OSINT monitoring
- Free, no credentials

**Limitations (important):**
- Public channels only
- Message IDs inferred (not authoritative)
- No reactions, accurate views, or replies
- No edit/deletion detection
- No media downloads
- Bias confidence: **low**

### Backend Comparison

| Capability | telethon | web |
|------------|----------|-----|
| Public channels | ✅ | ✅ |
| Private channels | ✅ | ❌ |
| Message text | ✅ | ✅ |
| Views/reactions | ✅ | ❌ |
| Edit detection | ✅ | ❌ |
| Media download | ✅ | ❌ |
| Resume by ID | ✅ | ❌ |
| Bias confidence | high | low |
| API required | Yes | No |

### Python API

```python
from tscrape import TelethonBackend, WebHTMLBackend

# Full API backend
async with TelethonBackend(api_id=ID, api_hash=HASH) as backend:
    async for item in backend.scrape_channel("@channel"):
        print(item.text)

# Web HTML backend (no API)
async with WebHTMLBackend() as backend:
    async for item in backend.scrape_channel("channel"):
        print(item.text)

    # Get bias disclosure for reports
    disclosure = backend.get_bias_disclosure()
    print(disclosure['disclaimer'])
```

### Bias Disclosure

Web backend automatically includes bias disclosure:

```json
{
  "backend": "web",
  "bias_confidence": "low",
  "known_limitations": [
    "Public channels only",
    "Message IDs inferred, not authoritative",
    "Edits not observable",
    "Reactions not captured"
  ],
  "disclaimer": "Data collected via HTML scraping..."
}
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

## Proxy Support

TScrape integrates with [Proxy-Hound](https://github.com/arandomguyhere/Proxy-Hound) and [SOCKS5-Scanner](https://github.com/arandomguyhere/Tools/tree/main/socks5-scanner) for automatic proxy rotation.

### Why Use Proxies?

- **Avoid IP bans** during large scrapes
- **Bypass rate limits** by rotating IPs on FloodWait
- **Geographic distribution** for better reliability

### Scraping with Proxies

```bash
# Enable proxy rotation (auto-loads from Proxy-Hound & SOCKS5-Scanner)
tscrape scrape @channel --proxy

# Filter by country
tscrape scrape @channel --proxy --proxy-country US --proxy-country DE

# Use custom proxy file
tscrape scrape @channel --proxy --proxy-file ./my_proxies.txt
```

### Proxy Management Commands

```bash
# Load and view proxy pool statistics
tscrape proxy load

# Load from specific source
tscrape proxy load --source socks5_scanner

# Load and test proxies
tscrape proxy load --test

# Test proxies from file
tscrape proxy test -f proxies.txt -o working.txt

# List available proxy sources
tscrape proxy sources
```

### Proxy Sources

| Source | Description |
|--------|-------------|
| `proxy_hound_socks5` | SOCKS5 from Proxy-Hound |
| `proxy_hound_socks4` | SOCKS4 from Proxy-Hound |
| `proxy_hound_https` | HTTPS from Proxy-Hound |
| `socks5_scanner` | SOCKS5 from SOCKS5-Scanner |

### Python API with Proxies

```python
from tscrape import TelegramScraper, ProxyManager, ProxyType

async def main():
    # Setup proxy manager
    proxy_manager = ProxyManager(
        preferred_types=[ProxyType.SOCKS5],
        preferred_countries=["US", "DE", "NL"]
    )
    await proxy_manager.load_from_sources()

    # Use with scraper
    async with TelegramScraper(
        api_id=API_ID,
        api_hash=API_HASH,
        proxy_manager=proxy_manager
    ) as scraper:
        async for msg in scraper.scrape_channel("@channel"):
            print(msg.text)

        # Check proxy stats
        print(scraper.get_proxy_stats())
```

### How Proxy Rotation Works

1. **On connect**: Selects best proxy from pool (weighted by success rate)
2. **On FloodWait**: After 2+ FloodWaits, rotates to a new proxy
3. **On failure**: Marks proxy as failed, tries another
4. **Dead proxies**: After 3 failures with <20% success rate, proxy is skipped

## Channel Discovery (Snowballing)

Discover related channels by analyzing message forwards. Based on the PLOS ONE methodology for network expansion.

### How It Works

1. Start with seed channels
2. Analyze messages for forwarded content
3. Extract source channels from forwards
4. Recursively discover channels at specified depth
5. Track forward counts to identify influential sources

### CLI Commands

```bash
# Discover channels from seeds
tscrape discover snowball @channel1 @channel2

# Deeper discovery (depth 2 = channels that forward to your discovered channels)
tscrape discover snowball @seed --depth 2 --max-channels 100

# Save discovered channels to file
tscrape discover snowball @seed -o discovered_channels.txt

# Build and export network graph
tscrape discover network @seed --format graphml -o network

# Export both GraphML and GEXF formats
tscrape discover network @seed @seed2 --depth 2 -f both -o channel_network
```

### Python API

```python
from tscrape import TelegramScraper, ChannelDiscovery

async def discover_network():
    async with TelegramScraper(api_id=ID, api_hash=HASH) as scraper:
        discovery = ChannelDiscovery(scraper)

        # Discover related channels
        channels = await discovery.snowball(
            seed_channels=["@channel1", "@channel2"],
            depth=2,
            message_limit=1000,
            max_channels=50
        )

        for ch in channels.values():
            print(f"{ch.username}: {ch.forward_count} forwards")

        # Export to Gephi format
        discovery.export_graphml("network.graphml")
        discovery.export_gexf("network.gexf")
```

### Network Visualization

Export formats are compatible with:
- **Gephi**: Open GraphML/GEXF files directly
- **NetworkX**: Load GraphML for Python analysis
- **Cytoscape**: Import GraphML for biological-style layouts

## Keyword Filtering

Filter scraped messages by keywords, patterns, and engagement metrics. Based on TelegramScrap paper methodology.

### CLI Commands

```bash
# Filter by keywords
tscrape filter channelname --keywords hack --keywords exploit

# Use regex patterns
tscrape filter channelname --regex "CVE-\d{4}-\d+" -o cves.json

# Use preset filters
tscrape filter channelname --preset cti -o threats.json   # Cyber threat intelligence
tscrape filter channelname --preset crypto -o crypto.json  # Cryptocurrency
tscrape filter channelname --preset viral                  # High-engagement posts

# Load keywords from file
tscrape filter channelname --keywords-file keywords.txt

# Combine criteria
tscrape filter channelname --keywords malware --min-views 1000 --mode all

# Export filtered results
tscrape filter channelname --preset cti -f csv -o filtered.csv
```

### Keyword File Format

```text
# Comments start with #
malware
ransomware
exploit

# Regex patterns wrapped in slashes
/CVE-\d{4}-\d+/
/bitcoin wallet: [a-zA-Z0-9]{26,35}/
```

### Preset Filters

| Preset | Description |
|--------|-------------|
| `cti` | Cyber threat intelligence (malware, exploits, IOCs) |
| `crypto` | Cryptocurrency (bitcoin, trading, wallets) |
| `viral` | High-engagement posts (10K+ views by default) |

### Python API

```python
from tscrape import MessageFilter, FilterMode, KeywordSet

# Custom filter
filter = MessageFilter(
    keywords=["hack", "exploit", "vulnerability"],
    keywords_regex=[r"CVE-\d{4}-\d+"],
    exclude_keywords=["game", "movie"],
    min_views=100,
    mode=FilterMode.ANY  # Match any keyword (OR)
)

# Use preset
cti_filter = KeywordSet.get_cti_filter()

# Apply to messages
for msg in messages:
    result = filter.matches(msg)
    if result.matched:
        print(f"Matched: {result.matched_keywords}")
```

## Bias Tracking & Methodology

TScrape implements academic-grade data quality tracking following computational social science and OSINT research standards. This enables transparent reporting of data collection limitations.

### Why Bias Tracking?

Telegram content is subject to:
- **Post-hoc deletion** - Messages may be removed after collection
- **Silent edits** - Content may change without notification
- **Access restrictions** - Some messages may be inaccessible
- **Sampling gaps** - Collection frequency affects completeness

TScrape quantifies these biases rather than ignoring them.

### Metrics Tracked

| Metric | Description |
|--------|-------------|
| **Gap Ratio** | Missing message IDs / expected IDs |
| **Deletion Rate** | Deleted messages / total observed |
| **Coverage Rate** | Observed / expected messages |
| **Edit Rate** | Edited messages / observed messages |
| **Sampling Latency** | Time between message creation and capture |

### CLI Commands

```bash
# View bias metrics for a channel
tscrape bias metrics mychannel

# Export comprehensive bias report (JSON)
tscrape bias report mychannel -o report.json

# View scrape run history
tscrape bias history

# Generate methodology statement for papers
tscrape bias statement mychannel
```

### Scrape Run Manifests

Each scrape generates a reproducibility manifest:

```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "tool_version": "tscrape 1.3.0",
  "telethon_version": "1.34.0",
  "python_version": "3.11.5",
  "start_time_utc": "2026-01-21T12:00:00Z",
  "end_time_utc": "2026-01-21T14:30:00Z",
  "channels": ["@channel1"],
  "scrape_mode": "incremental",
  "runtime_stats": {
    "messages_collected": 15420,
    "flood_waits": 3,
    "errors_encountered": 2
  }
}
```

### Methodology Statement Generator

TScrape generates academic-ready methodology statements:

```bash
$ tscrape bias statement mychannel

Data collection for channel 'mychannel' occurred between 2026-01-15 and
2026-01-21. Approximately 2.3% of message IDs within the observed range
were unavailable at collection time, consistent with deletion or access
restrictions. The confirmed deletion rate was 0.8%. Approximately 1.2%
of collected messages showed evidence of post-publication editing.
```

### Python API

```python
from tscrape import TelegramScraper, BiasTracker, BiasMetrics

async def scrape_with_tracking():
    async with TelegramScraper(api_id=ID, api_hash=HASH) as scraper:
        # Scraping automatically tracks bias
        async for msg in scraper.scrape_channel("@channel"):
            pass

        # Get bias metrics
        metrics = scraper.get_bias_metrics(channel_id, "channel")
        print(f"Coverage: {metrics['coverage']['coverage_rate']:.1%}")
        print(f"Gap ratio: {metrics['coverage']['gap_ratio']:.1%}")

        # Generate methodology statement
        statement = scraper.get_methodology_statement(channel_id, "channel")
        print(statement)

        # Export full report
        scraper.export_bias_report(channel_id, "channel", "bias_report.json")
```

### Schema Details

**Message Continuity Table** - Tracks expected vs observed messages:
```sql
message_continuity (
    channel_id, expected_msg_id, observed,
    first_seen_ts, last_checked_ts, status
)
-- status: observed | deleted | inaccessible | unknown | edited
```

**Message Status History** - Tracks changes over time:
```sql
message_status_history (
    channel_id, message_id, observed_ts,
    status, text_checksum, text_length
)
```

**Scrape Runs** - Reproducibility manifests:
```sql
scrape_runs (run_id, manifest_json, created_at)
```

## Architecture

```
tscrape/
├── __init__.py       # Package exports
├── scraper.py        # Main TelegramScraper class
├── storage.py        # Parquet + SQLite storage
├── media.py          # Parallel media downloader
├── proxy.py          # Proxy manager with rotation
├── discovery.py      # Channel discovery (snowballing)
├── filters.py        # Keyword filtering
├── bias.py           # Bias tracking & reproducibility
├── models.py         # Data models (ScrapedMessage, etc.)
├── config.py         # Configuration management
├── cli.py            # Command-line interface
└── backends/         # Pluggable scraping backends
    ├── __init__.py
    ├── base.py           # Abstract base class
    ├── telethon_backend.py   # Full API (default)
    └── web_backend.py    # HTML scraping (no API)

data/
├── tscrape_state.db  # SQLite state (checkpoints, bias tracking)
├── manifest_*.json   # Scrape run manifests
├── network.graphml   # Channel network (optional)
└── channelname/
    ├── messages_*.parquet  # Message data
    ├── bias_report.json    # Bias report (optional)
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

Proxy support powered by:
- [Proxy-Hound](https://github.com/arandomguyhere/Proxy-Hound)
- [SOCKS5-Scanner](https://github.com/arandomguyhere/Tools/tree/main/socks5-scanner)

Academic foundations:
- TelegramScrap (arXiv:2412.16786) - Keyword filtering methodology
- CTI Dataset Construction (arXiv:2509.20943) - Threat intelligence classification
- PLOS ONE Narrative Analysis - Snowballing channel discovery
- Computational social science best practices - Bias tracking schema
- Digital trace data collection guidelines - Reproducibility standards
