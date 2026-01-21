"""
CLI interface for TScrape.

Provides a user-friendly command-line interface with:
- Interactive channel selection
- Progress bars
- Multiple commands (scrape, export, list, stats)
- Proxy management (load, test, stats)
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional, List

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from .scraper import TelegramScraper
from .storage import StorageManager
from .config import Config
from .proxy import ProxyManager, ProxyType
from .discovery import ChannelDiscovery
from .filters import MessageFilter, FilterMode, KeywordSet, create_filter_from_file

console = Console()


def setup_logging(level: str, log_file: Optional[str] = None) -> None:
    """Configure logging."""
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers
    )


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Config file path')
@click.option('--data-dir', '-d', default='./data', help='Data directory')
@click.option('--log-level', '-l', default='INFO', help='Log level')
@click.pass_context
def cli(ctx, config, data_dir, log_level):
    """
    TScrape - Modern Telegram Channel Scraper (2026)

    A high-performance scraper combining the best practices from
    Telethon, with Parquet storage and parallel media downloads.
    """
    ctx.ensure_object(dict)

    if config:
        ctx.obj['config'] = Config.from_file(Path(config))
    else:
        ctx.obj['config'] = Config.from_env()

    ctx.obj['config'].data_dir = data_dir
    ctx.obj['config'].log_level = log_level

    setup_logging(log_level)


@cli.command()
@click.argument('channel')
@click.option('--limit', '-n', type=int, help='Maximum messages to scrape')
@click.option('--media/--no-media', default=False, help='Download media files')
@click.option('--resume/--no-resume', default=True, help='Resume from checkpoint')
@click.option('--api-id', type=int, envvar='TELEGRAM_API_ID', help='Telegram API ID')
@click.option('--api-hash', envvar='TELEGRAM_API_HASH', help='Telegram API Hash')
@click.option('--proxy/--no-proxy', default=False, help='Use proxy rotation')
@click.option('--proxy-file', type=click.Path(exists=True), help='Load proxies from file')
@click.option('--proxy-country', '-pc', multiple=True, help='Filter proxies by country code (e.g., US, DE)')
@click.pass_context
def scrape(ctx, channel, limit, media, resume, api_id, api_hash, proxy, proxy_file, proxy_country):
    """
    Scrape messages from a Telegram channel.

    CHANNEL can be a username (@channel) or ID (1234567890).

    Examples:

        tscrape scrape @durov

        tscrape scrape @mychannel --limit 1000 --media

        tscrape scrape @mychannel --proxy

        tscrape scrape @mychannel --proxy --proxy-country US --proxy-country DE

        tscrape scrape 1234567890 --no-resume
    """
    config = ctx.obj['config']

    # Get credentials
    api_id = api_id or config.api_id
    api_hash = api_hash or config.api_hash

    if not api_id or not api_hash:
        console.print("[red]Error: API credentials required[/red]")
        console.print("Set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables")
        console.print("Or use --api-id and --api-hash options")
        console.print("\nGet credentials at: https://my.telegram.org")
        raise SystemExit(1)

    asyncio.run(_scrape_channel(
        api_id=api_id,
        api_hash=api_hash,
        channel=channel,
        limit=limit,
        download_media=media,
        resume=resume,
        config=config,
        use_proxy=proxy,
        proxy_file=proxy_file,
        proxy_countries=list(proxy_country) if proxy_country else None
    ))


async def _scrape_channel(
    api_id: int,
    api_hash: str,
    channel: str,
    limit: Optional[int],
    download_media: bool,
    resume: bool,
    config: Config,
    use_proxy: bool = False,
    proxy_file: Optional[str] = None,
    proxy_countries: Optional[List[str]] = None
):
    """Internal async scrape implementation."""
    console.print(Panel.fit(
        f"[bold blue]TScrape[/bold blue] - Scraping [green]{channel}[/green]",
        subtitle="Press Ctrl+C to stop gracefully"
    ))

    # Setup proxy manager if requested
    proxy_manager = None
    if use_proxy:
        console.print("[cyan]Loading proxies...[/cyan]")

        proxy_manager = ProxyManager(
            preferred_types=[ProxyType.SOCKS5, ProxyType.SOCKS4],
            preferred_countries=proxy_countries
        )

        if proxy_file:
            count = proxy_manager.load_from_file(Path(proxy_file))
            console.print(f"[green]Loaded {count} proxies from file[/green]")
        else:
            # Load from Proxy-Hound and SOCKS5-Scanner
            count = await proxy_manager.load_from_sources()
            console.print(f"[green]Loaded {count} proxies from remote sources[/green]")

        if proxy_manager.available_count == 0:
            console.print("[yellow]Warning: No proxies available, continuing without proxy[/yellow]")
            proxy_manager = None
        else:
            stats = proxy_manager.get_stats()
            console.print(f"[dim]Available: {stats['available']} | By type: {stats['by_type']}[/dim]")

    async with TelegramScraper(
        api_id=api_id,
        api_hash=api_hash,
        data_dir=config.data_dir,
        config=config,
        proxy_manager=proxy_manager
    ) as scraper:

        # Get channel info
        try:
            info = await scraper.get_channel_info(channel)
            console.print(f"\n[bold]Channel:[/bold] {info.title}")
            if info.username:
                console.print(f"[bold]Username:[/bold] @{info.username}")
            if info.participants_count:
                console.print(f"[bold]Members:[/bold] {info.participants_count:,}")
            console.print()

        except Exception as e:
            console.print(f"[red]Error getting channel info: {e}[/red]")
            raise SystemExit(1)

        # Progress tracking
        message_count = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True
        ) as progress:

            task = progress.add_task(
                f"Scraping {channel}...",
                total=limit or 100  # Estimate if no limit
            )

            try:
                async for msg in scraper.scrape_channel(
                    channel=channel,
                    limit=limit,
                    download_media=download_media,
                    resume=resume
                ):
                    message_count += 1

                    # Update progress
                    if limit:
                        progress.update(task, completed=message_count)
                    else:
                        progress.update(task, advance=1, total=message_count + 100)

            except KeyboardInterrupt:
                console.print("\n[yellow]Stopping gracefully...[/yellow]")
                scraper.stop()

        # Print summary
        summary = f"[green]Scraped {message_count:,} messages[/green]\n"
        summary += f"Data saved to: {config.data_dir}/{info.username or info.id}/"

        # Add proxy stats if used
        proxy_stats = scraper.get_proxy_stats()
        if proxy_stats:
            summary += f"\n[dim]Proxy: {proxy_stats.get('current_proxy', 'N/A')}[/dim]"

        console.print(Panel.fit(summary, title="Complete"))


@cli.command()
@click.option('--api-id', type=int, envvar='TELEGRAM_API_ID', help='Telegram API ID')
@click.option('--api-hash', envvar='TELEGRAM_API_HASH', help='Telegram API Hash')
@click.option('--limit', '-n', type=int, default=50, help='Maximum channels to list')
@click.pass_context
def channels(ctx, api_id, api_hash, limit):
    """List accessible channels and groups."""
    config = ctx.obj['config']

    api_id = api_id or config.api_id
    api_hash = api_hash or config.api_hash

    if not api_id or not api_hash:
        console.print("[red]Error: API credentials required[/red]")
        raise SystemExit(1)

    asyncio.run(_list_channels(api_id, api_hash, limit, config))


async def _list_channels(api_id: int, api_hash: str, limit: int, config: Config):
    """List accessible channels."""
    async with TelegramScraper(
        api_id=api_id,
        api_hash=api_hash,
        data_dir=config.data_dir,
        config=config
    ) as scraper:

        dialogs = await scraper.get_dialogs(limit=limit)

        table = Table(title="Accessible Channels & Groups")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Username", style="yellow")
        table.add_column("Type", style="magenta")

        for dialog in dialogs:
            table.add_row(
                str(dialog['id']),
                dialog['name'],
                f"@{dialog['username']}" if dialog['username'] else "-",
                "Channel" if dialog['is_channel'] else "Group"
            )

        console.print(table)


@cli.command()
@click.argument('channel')
@click.option('--format', '-f', 'fmt', type=click.Choice(['json', 'csv', 'parquet']),
              default='parquet', help='Export format')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.pass_context
def export(ctx, channel, fmt, output):
    """
    Export scraped data to file.

    Examples:

        tscrape export mychannel --format json

        tscrape export mychannel -f csv -o /tmp/export.csv
    """
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    output_path = Path(output) if output else None

    if fmt == 'json':
        path = storage.export_json(channel, output_path)
    elif fmt == 'csv':
        path = storage.export_csv(channel, output_path)
    else:
        path = storage.export_parquet(channel, output_path)

    console.print(f"[green]Exported to: {path}[/green]")


@cli.command()
@click.argument('channel')
@click.pass_context
def stats(ctx, channel):
    """Show statistics for a scraped channel."""
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    statistics = storage.get_stats(channel)

    if statistics['messages'] == 0:
        console.print(f"[yellow]No data found for channel: {channel}[/yellow]")
        return

    table = Table(title=f"Statistics for {channel}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Messages", f"{statistics['messages']:,}")
    table.add_row("Unique Senders", f"{statistics['unique_senders']:,}")
    table.add_row("Total Views", f"{statistics['total_views']:,}")
    table.add_row("Total Forwards", f"{statistics['total_forwards']:,}")
    table.add_row("Messages with Media", f"{statistics['media_count']:,}")
    table.add_row("Pinned Messages", f"{statistics['pinned_count']:,}")

    if statistics['date_range']['oldest']:
        table.add_row("Oldest Message", statistics['date_range']['oldest'][:10])
        table.add_row("Newest Message", statistics['date_range']['newest'][:10])

    console.print(table)


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize configuration file interactively."""
    console.print(Panel.fit(
        "[bold blue]TScrape Setup[/bold blue]\n\n"
        "This will create a configuration file with your API credentials.\n"
        "Get your credentials at: https://my.telegram.org",
        title="Welcome"
    ))

    api_id = Prompt.ask("Enter your API ID")
    api_hash = Prompt.ask("Enter your API Hash")
    data_dir = Prompt.ask("Data directory", default="./data")

    config = Config(
        api_id=int(api_id),
        api_hash=api_hash,
        data_dir=data_dir
    )

    config_path = Path("tscrape_config.json")
    config.to_file(config_path)

    console.print(f"\n[green]Configuration saved to: {config_path}[/green]")
    console.print("\nYou can now run:")
    console.print(f"  [cyan]tscrape -c {config_path} scrape @channel[/cyan]")


@cli.group()
def discover():
    """Channel discovery commands (snowballing)."""
    pass


@discover.command("snowball")
@click.argument('channels', nargs=-1, required=True)
@click.option('--depth', '-d', type=int, default=1, help='Discovery depth (1-3 recommended)')
@click.option('--limit', '-n', type=int, default=500, help='Messages to scan per channel')
@click.option('--max-channels', '-m', type=int, default=100, help='Maximum channels to discover')
@click.option('--min-forwards', type=int, default=3, help='Minimum forwards to consider a channel')
@click.option('--api-id', type=int, envvar='TELEGRAM_API_ID', help='Telegram API ID')
@click.option('--api-hash', envvar='TELEGRAM_API_HASH', help='Telegram API Hash')
@click.option('--output', '-o', type=click.Path(), help='Save discovered channels to file')
@click.pass_context
def discover_snowball(ctx, channels, depth, limit, max_channels, min_forwards, api_id, api_hash, output):
    """
    Discover related channels via forward analysis (snowballing).

    Based on PLOS ONE methodology for network expansion.

    Examples:

        tscrape discover snowball @durov @telegram

        tscrape discover snowball @mychannel --depth 2 --max-channels 50

        tscrape discover snowball @seed1 @seed2 -o discovered.txt
    """
    config = ctx.obj['config']

    api_id = api_id or config.api_id
    api_hash = api_hash or config.api_hash

    if not api_id or not api_hash:
        console.print("[red]Error: API credentials required[/red]")
        raise SystemExit(1)

    asyncio.run(_discover_snowball(
        api_id=api_id,
        api_hash=api_hash,
        channels=list(channels),
        depth=depth,
        limit=limit,
        max_channels=max_channels,
        min_forwards=min_forwards,
        output=output,
        config=config
    ))


async def _discover_snowball(
    api_id: int,
    api_hash: str,
    channels: List[str],
    depth: int,
    limit: int,
    max_channels: int,
    min_forwards: int,
    output: Optional[str],
    config: Config
):
    """Internal snowball discovery implementation."""
    console.print(Panel.fit(
        f"[bold blue]Channel Discovery[/bold blue]\n\n"
        f"Seed channels: {', '.join(channels)}\n"
        f"Depth: {depth} | Max channels: {max_channels}",
        title="Snowballing"
    ))

    async with TelegramScraper(
        api_id=api_id,
        api_hash=api_hash,
        data_dir=config.data_dir,
        config=config
    ) as scraper:

        discovery = ChannelDiscovery(scraper)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True
        ) as progress:
            task = progress.add_task("Discovering channels...", total=None)

            discovered = await discovery.snowball(
                seed_channels=channels,
                depth=depth,
                message_limit=limit,
                max_channels=max_channels,
                min_forward_count=min_forwards
            )

            progress.update(task, completed=True)

        # Display results
        table = Table(title=f"Discovered {len(discovered)} Channels")
        table.add_column("Username/ID", style="cyan")
        table.add_column("Title", style="green")
        table.add_column("Members", style="yellow", justify="right")
        table.add_column("Forwards", style="magenta", justify="right")
        table.add_column("Depth", style="dim", justify="right")

        for ch in sorted(discovered.values(), key=lambda x: x.forward_count, reverse=True)[:20]:
            table.add_row(
                ch.username or str(ch.id),
                (ch.title[:30] + "...") if len(ch.title) > 30 else ch.title,
                f"{ch.participants:,}" if ch.participants else "-",
                str(ch.forward_count),
                str(ch.depth)
            )

        console.print(table)

        if len(discovered) > 20:
            console.print(f"[dim]... and {len(discovered) - 20} more channels[/dim]")

        # Save to file if requested
        if output:
            with open(output, 'w') as f:
                for ch in discovered.values():
                    line = ch.username or str(ch.id)
                    f.write(f"{line}\n")
            console.print(f"\n[green]Saved {len(discovered)} channels to {output}[/green]")

        # Show network stats
        console.print(f"\n[bold]Network Stats:[/bold]")
        console.print(f"  Channels discovered: {len(discovered)}")
        console.print(f"  Edges (forward links): {len(discovery._edges)}")


@discover.command("network")
@click.argument('channels', nargs=-1, required=True)
@click.option('--depth', '-d', type=int, default=1, help='Discovery depth')
@click.option('--limit', '-n', type=int, default=500, help='Messages to scan per channel')
@click.option('--format', '-f', 'fmt', type=click.Choice(['graphml', 'gexf', 'both']),
              default='graphml', help='Export format')
@click.option('--output', '-o', type=click.Path(), help='Output file path (without extension)')
@click.option('--api-id', type=int, envvar='TELEGRAM_API_ID', help='Telegram API ID')
@click.option('--api-hash', envvar='TELEGRAM_API_HASH', help='Telegram API Hash')
@click.pass_context
def discover_network(ctx, channels, depth, limit, fmt, output, api_id, api_hash):
    """
    Build and export channel network graph.

    Exports to GraphML or GEXF for visualization in Gephi.

    Examples:

        tscrape discover network @channel1 @channel2 --format graphml

        tscrape discover network @seed -d 2 -f both -o my_network
    """
    config = ctx.obj['config']

    api_id = api_id or config.api_id
    api_hash = api_hash or config.api_hash

    if not api_id or not api_hash:
        console.print("[red]Error: API credentials required[/red]")
        raise SystemExit(1)

    asyncio.run(_discover_network(
        api_id=api_id,
        api_hash=api_hash,
        channels=list(channels),
        depth=depth,
        limit=limit,
        fmt=fmt,
        output=output,
        config=config
    ))


async def _discover_network(
    api_id: int,
    api_hash: str,
    channels: List[str],
    depth: int,
    limit: int,
    fmt: str,
    output: Optional[str],
    config: Config
):
    """Internal network export implementation."""
    console.print(Panel.fit(
        f"[bold blue]Network Graph Export[/bold blue]\n\n"
        f"Building network from: {', '.join(channels)}",
        title="Discovery"
    ))

    async with TelegramScraper(
        api_id=api_id,
        api_hash=api_hash,
        data_dir=config.data_dir,
        config=config
    ) as scraper:

        discovery = ChannelDiscovery(scraper)

        with console.status("[cyan]Discovering channel network..."):
            await discovery.snowball(
                seed_channels=channels,
                depth=depth,
                message_limit=limit
            )

        # Export
        output_base = Path(output) if output else Path(config.data_dir) / "network"

        if fmt in ('graphml', 'both'):
            path = discovery.export_graphml(output_base.with_suffix('.graphml'))
            console.print(f"[green]GraphML exported to: {path}[/green]")

        if fmt in ('gexf', 'both'):
            path = discovery.export_gexf(output_base.with_suffix('.gexf'))
            console.print(f"[green]GEXF exported to: {path}[/green]")

        console.print(f"\n[bold]Network:[/bold] {len(discovery._discovered)} nodes, {len(discovery._edges)} edges")
        console.print("[dim]Open in Gephi for visualization[/dim]")


@cli.command()
@click.argument('channel')
@click.option('--keywords', '-k', multiple=True, help='Keywords to search for')
@click.option('--keywords-file', type=click.Path(exists=True), help='Load keywords from file')
@click.option('--regex', '-r', multiple=True, help='Regex patterns to match')
@click.option('--exclude', '-e', multiple=True, help='Keywords to exclude')
@click.option('--preset', type=click.Choice(['cti', 'crypto', 'viral']), help='Use predefined keyword set')
@click.option('--min-views', type=int, help='Minimum view count')
@click.option('--min-date', type=click.DateTime(), help='Only messages after this date')
@click.option('--max-date', type=click.DateTime(), help='Only messages before this date')
@click.option('--mode', type=click.Choice(['all', 'any']), default='any', help='Filter combination mode')
@click.option('--output', '-o', type=click.Path(), help='Output file for filtered messages')
@click.option('--format', '-f', 'fmt', type=click.Choice(['json', 'csv', 'parquet']),
              default='json', help='Output format')
@click.pass_context
def filter(ctx, channel, keywords, keywords_file, regex, exclude, preset, min_views, min_date, max_date, mode, output, fmt):
    """
    Filter scraped messages by keywords and criteria.

    Based on TelegramScrap paper methodology.

    Examples:

        tscrape filter mychannel --keywords hack --keywords exploit

        tscrape filter mychannel --preset cti -o threats.json

        tscrape filter mychannel --keywords-file keywords.txt --min-views 1000

        tscrape filter mychannel --regex "CVE-\\d{4}-\\d+" -o cves.json
    """
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    # Load messages
    console.print(f"[cyan]Loading messages from {channel}...[/cyan]")
    df = storage.load_messages(channel)

    if df.empty:
        console.print(f"[yellow]No data found for channel: {channel}[/yellow]")
        return

    console.print(f"[green]Loaded {len(df):,} messages[/green]")

    # Build filter
    if keywords_file:
        msg_filter = create_filter_from_file(keywords_file)
    elif preset:
        if preset == 'cti':
            msg_filter = KeywordSet.get_cti_filter()
        elif preset == 'crypto':
            msg_filter = KeywordSet.get_crypto_filter()
        else:  # viral
            msg_filter = KeywordSet.get_viral_filter(min_views or 10000)
    else:
        filter_mode = FilterMode.ALL if mode == 'all' else FilterMode.ANY
        msg_filter = MessageFilter(
            keywords=list(keywords) if keywords else None,
            keywords_regex=list(regex) if regex else None,
            exclude_keywords=list(exclude) if exclude else None,
            min_views=min_views,
            min_date=min_date,
            max_date=max_date,
            mode=filter_mode
        )

    # Convert DataFrame rows to minimal dict for filtering
    from .models import ScrapedMessage
    import json as json_lib

    matched_rows = []
    matched_keywords_all = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
        transient=True
    ) as progress:
        task = progress.add_task("Filtering messages...", total=len(df))

        for idx, row in df.iterrows():
            # Create minimal message for filtering
            reactions = []
            if 'reactions_json' in row and row['reactions_json']:
                try:
                    reactions = json_lib.loads(row['reactions_json'])
                except:
                    pass

            msg = ScrapedMessage(
                message_id=row.get('message_id', 0),
                channel_id=row.get('channel_id', 0),
                channel_name=channel,
                date=row.get('date'),
                text=row.get('text', '') or '',
                raw_text=row.get('raw_text', '') or '',
                views=row.get('views', 0) or 0,
                forwards=row.get('forwards', 0) or 0,
                reactions=reactions,
                has_media=row.get('has_media', False),
                media_type=row.get('media_type')
            )

            result = msg_filter.matches(msg)
            if result.matched:
                matched_rows.append(idx)
                matched_keywords_all.extend(result.matched_keywords)

            progress.update(task, advance=1)

    # Filter DataFrame
    filtered_df = df.loc[matched_rows]

    # Show results
    stats = msg_filter.get_stats()
    console.print(f"\n[bold]Filter Results:[/bold]")
    console.print(f"  Matched: [green]{stats['total_matched']:,}[/green] / {stats['total_checked']:,}")
    console.print(f"  Match rate: {stats['match_rate']:.1%}")

    if matched_keywords_all:
        from collections import Counter
        top_keywords = Counter(matched_keywords_all).most_common(10)
        console.print(f"\n[bold]Top matched keywords:[/bold]")
        for kw, count in top_keywords:
            console.print(f"  {kw}: {count}")

    # Export if requested
    if output:
        output_path = Path(output)
        if fmt == 'json':
            records = filtered_df.to_dict(orient='records')
            import json as json_mod
            for record in records:
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):
                        record[key] = value.isoformat()
            with open(output_path, 'w', encoding='utf-8') as f:
                json_mod.dump(records, f, ensure_ascii=False, indent=2, default=str)
        elif fmt == 'csv':
            filtered_df.to_csv(output_path, index=False)
        else:
            filtered_df.to_parquet(output_path, index=False)

        console.print(f"\n[green]Exported {len(filtered_df)} messages to {output_path}[/green]")
    else:
        # Show sample of matched messages
        if not filtered_df.empty:
            console.print(f"\n[bold]Sample matched messages:[/bold]")
            for _, row in filtered_df.head(5).iterrows():
                text = (row.get('text', '') or '')[:100]
                if len(row.get('text', '') or '') > 100:
                    text += "..."
                console.print(f"  [dim]{str(row.get('date', ''))[:10]}[/dim] {text}")


@cli.group()
def proxy():
    """Proxy management commands."""
    pass


@proxy.command("load")
@click.option('--source', '-s', multiple=True,
              help='Proxy source (proxy_hound_socks5, socks5_scanner, etc.)')
@click.option('--file', '-f', type=click.Path(exists=True), help='Load from local file')
@click.option('--test/--no-test', default=False, help='Test proxies after loading')
@click.pass_context
def proxy_load(ctx, source, file, test):
    """
    Load proxies from remote sources or file.

    Sources available:
    - proxy_hound_socks5, proxy_hound_socks4, proxy_hound_https
    - socks5_scanner

    Examples:

        tscrape proxy load

        tscrape proxy load --source socks5_scanner --test

        tscrape proxy load --file ./my_proxies.txt
    """
    asyncio.run(_proxy_load(list(source) if source else None, file, test))


async def _proxy_load(sources: Optional[List[str]], file: Optional[str], test: bool):
    """Load proxies implementation."""
    proxy_manager = ProxyManager()

    with console.status("[cyan]Loading proxies..."):
        if file:
            count = proxy_manager.load_from_file(Path(file))
            console.print(f"[green]Loaded {count} proxies from {file}[/green]")
        else:
            count = await proxy_manager.load_from_sources(sources)
            console.print(f"[green]Loaded {count} proxies from remote sources[/green]")

    # Show stats
    stats = proxy_manager.get_stats()

    table = Table(title="Proxy Pool Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Proxies", str(stats['total']))
    table.add_row("Available", str(stats['available']))

    for ptype, pcount in stats['by_type'].items():
        table.add_row(f"  {ptype.upper()}", str(pcount))

    if stats['by_country']:
        top_countries = sorted(stats['by_country'].items(), key=lambda x: x[1], reverse=True)[:5]
        table.add_row("Top Countries", ", ".join(f"{c}:{n}" for c, n in top_countries))

    console.print(table)

    # Test if requested
    if test and proxy_manager.count > 0:
        console.print("\n[cyan]Testing proxies...[/cyan]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Testing...", total=proxy_manager.count)

            results = await proxy_manager.test_all_proxies(max_concurrent=50)

            progress.update(task, completed=proxy_manager.count)

        console.print(f"\n[green]Working: {results['working']}[/green] | "
                     f"[red]Dead: {results['dead']}[/red] | "
                     f"Success rate: {results['success_rate']:.1%}")


@proxy.command("test")
@click.option('--file', '-f', type=click.Path(exists=True), required=True, help='Proxy file to test')
@click.option('--concurrent', '-c', type=int, default=50, help='Concurrent connections')
@click.option('--output', '-o', type=click.Path(), help='Save working proxies to file')
@click.pass_context
def proxy_test(ctx, file, concurrent, output):
    """
    Test proxies from a file.

    Examples:

        tscrape proxy test -f proxies.txt

        tscrape proxy test -f proxies.txt -o working.txt -c 100
    """
    asyncio.run(_proxy_test(file, concurrent, output))


async def _proxy_test(file: str, concurrent: int, output: Optional[str]):
    """Test proxies implementation."""
    proxy_manager = ProxyManager()

    console.print(f"[cyan]Loading proxies from {file}...[/cyan]")
    count = proxy_manager.load_from_file(Path(file))
    console.print(f"[green]Loaded {count} proxies[/green]")

    console.print(f"\n[cyan]Testing with {concurrent} concurrent connections...[/cyan]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Testing proxies...", total=count)
        results = await proxy_manager.test_all_proxies(max_concurrent=concurrent)
        progress.update(task, completed=count)

    console.print(f"\n[bold]Results:[/bold]")
    console.print(f"  [green]Working: {results['working']}[/green]")
    console.print(f"  [red]Dead: {results['dead']}[/red]")
    console.print(f"  Success rate: {results['success_rate']:.1%}")

    if output:
        working = [p for p in proxy_manager._proxies if not p.is_dead]
        with open(output, 'w') as f:
            for p in working:
                f.write(f"{p.host}:{p.port}\n")
        console.print(f"\n[green]Saved {len(working)} working proxies to {output}[/green]")


@proxy.command("sources")
def proxy_sources():
    """List available proxy sources."""
    from .proxy import PROXY_SOURCES

    table = Table(title="Available Proxy Sources")
    table.add_column("Name", style="cyan")
    table.add_column("URL", style="dim")

    for name, url in PROXY_SOURCES.items():
        table.add_row(name, url)

    console.print(table)
    console.print("\n[dim]Use with: tscrape proxy load --source <name>[/dim]")


@cli.group()
def bias():
    """Bias tracking and data quality commands."""
    pass


@bias.command("metrics")
@click.argument('channel')
@click.pass_context
def bias_metrics(ctx, channel):
    """
    Show bias metrics for a scraped channel.

    Displays gap ratio, deletion rate, coverage, and other
    academic-grade data quality indicators.

    Examples:

        tscrape bias metrics mychannel
    """
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    # Get channel ID from state
    with storage._get_connection() as conn:
        row = conn.execute(
            "SELECT channel_id FROM scrape_state WHERE channel_name = ?",
            (channel,)
        ).fetchone()

        if not row:
            console.print(f"[yellow]No scrape data found for channel: {channel}[/yellow]")
            return

        channel_id = row['channel_id']

    metrics = storage.get_bias_metrics(channel_id, channel)

    if not metrics:
        console.print("[yellow]Bias tracking not enabled or no data available[/yellow]")
        return

    # Display metrics
    table = Table(title=f"Bias Metrics for {channel}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    # Coverage
    table.add_row("Expected Messages", f"{metrics.expected_message_count:,}")
    table.add_row("Observed Messages", f"{metrics.observed_message_count:,}")
    table.add_row("Gap Count", f"{metrics.gap_count:,}")
    table.add_row("Coverage Rate", f"{metrics.coverage_rate:.1%}")
    table.add_row("Gap Ratio", f"{metrics.gap_ratio:.1%}")

    # Deletions
    table.add_row("Confirmed Deleted", f"{metrics.confirmed_deleted:,}")
    table.add_row("Possibly Deleted", f"{metrics.possibly_deleted:,}")
    table.add_row("Deletion Rate", f"{metrics.deletion_rate:.1%}")

    # Edits
    table.add_row("Edited Messages", f"{metrics.edited_messages:,}")
    table.add_row("Edit Rate", f"{metrics.edit_rate:.1%}")

    console.print(table)

    # Show methodology statement
    console.print(f"\n[bold]Methodology Statement:[/bold]")
    console.print(Panel(metrics.get_methodology_statement(), border_style="dim"))


@bias.command("report")
@click.argument('channel')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.pass_context
def bias_report(ctx, channel, output):
    """
    Export comprehensive bias report for a channel.

    Generates a JSON report with all bias metrics, run history,
    and methodology statement for academic papers.

    Examples:

        tscrape bias report mychannel

        tscrape bias report mychannel -o my_report.json
    """
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    # Get channel ID
    with storage._get_connection() as conn:
        row = conn.execute(
            "SELECT channel_id FROM scrape_state WHERE channel_name = ?",
            (channel,)
        ).fetchone()

        if not row:
            console.print(f"[yellow]No scrape data found for channel: {channel}[/yellow]")
            return

        channel_id = row['channel_id']

    output_path = Path(output) if output else None
    report_path = storage.export_bias_report(channel_id, channel, output_path)

    if report_path:
        console.print(f"[green]Bias report exported to: {report_path}[/green]")
    else:
        console.print("[yellow]Bias tracking not enabled or no data available[/yellow]")


@bias.command("history")
@click.option('--limit', '-n', type=int, default=10, help='Number of runs to show')
@click.pass_context
def bias_history(ctx, limit):
    """
    Show scrape run history for reproducibility tracking.

    Displays recent scrape runs with their manifests.

    Examples:

        tscrape bias history

        tscrape bias history --limit 5
    """
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    history = storage.get_scrape_history(limit)

    if not history:
        console.print("[yellow]No scrape history found[/yellow]")
        return

    table = Table(title="Scrape Run History")
    table.add_column("Run ID", style="cyan")
    table.add_column("Date", style="green")
    table.add_column("Channels", style="yellow")
    table.add_column("Messages", style="magenta", justify="right")
    table.add_column("Errors", style="red", justify="right")
    table.add_column("FloodWaits", style="dim", justify="right")

    for run in history:
        stats = run.get('runtime_stats', {})
        start = run.get('start_time_utc', '')[:10] if run.get('start_time_utc') else '-'

        table.add_row(
            run.get('run_id', '')[:8],
            start,
            ', '.join(run.get('channels', []))[:30],
            str(stats.get('messages_collected', 0)),
            str(stats.get('errors_encountered', 0)),
            str(stats.get('flood_waits', 0))
        )

    console.print(table)


@bias.command("statement")
@click.argument('channel')
@click.pass_context
def bias_statement(ctx, channel):
    """
    Generate methodology statement for academic papers.

    Outputs a formatted statement suitable for inclusion in
    the methodology section of research publications.

    Examples:

        tscrape bias statement mychannel
    """
    config = ctx.obj['config']
    storage = StorageManager(Path(config.data_dir))

    # Get channel ID
    with storage._get_connection() as conn:
        row = conn.execute(
            "SELECT channel_id FROM scrape_state WHERE channel_name = ?",
            (channel,)
        ).fetchone()

        if not row:
            console.print(f"[yellow]No scrape data found for channel: {channel}[/yellow]")
            return

        channel_id = row['channel_id']

    statement = storage.get_methodology_statement(channel_id, channel)

    if statement:
        console.print(Panel(
            statement,
            title="Methodology Statement",
            subtitle="Copy to your paper's methodology section"
        ))
    else:
        console.print("[yellow]Bias tracking not enabled or no data available[/yellow]")


def main():
    """Entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
