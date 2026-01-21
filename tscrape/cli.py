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


def main():
    """Entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
