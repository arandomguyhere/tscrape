"""
CLI interface for TScrape.

Provides a user-friendly command-line interface with:
- Interactive channel selection
- Progress bars
- Multiple commands (scrape, export, list, stats)
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
@click.pass_context
def scrape(ctx, channel, limit, media, resume, api_id, api_hash):
    """
    Scrape messages from a Telegram channel.

    CHANNEL can be a username (@channel) or ID (1234567890).

    Examples:

        tscrape scrape @duloruv

        tscrape scrape @mychannel --limit 1000 --media

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
        config=config
    ))


async def _scrape_channel(
    api_id: int,
    api_hash: str,
    channel: str,
    limit: Optional[int],
    download_media: bool,
    resume: bool,
    config: Config
):
    """Internal async scrape implementation."""
    console.print(Panel.fit(
        f"[bold blue]TScrape[/bold blue] - Scraping [green]{channel}[/green]",
        subtitle="Press Ctrl+C to stop gracefully"
    ))

    async with TelegramScraper(
        api_id=api_id,
        api_hash=api_hash,
        data_dir=config.data_dir,
        config=config
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
        console.print(Panel.fit(
            f"[green]Scraped {message_count:,} messages[/green]\n"
            f"Data saved to: {config.data_dir}/{info.username or info.id}/",
            title="Complete"
        ))


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


def main():
    """Entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
