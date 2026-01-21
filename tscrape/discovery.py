"""
Channel Discovery Module for TScrape.

Implements the "snowballing" method from academic research:
- PLOS ONE: "A computational analysis of Telegram's narrative affordances"
- arXiv:2412.16786: TelegramScrap methodology

Features:
- Discover related channels via message forwards
- Build channel networks/graphs
- Export to graph formats (GraphML, GEXF, JSON)
"""

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Set, Any, AsyncGenerator
from pathlib import Path
import json

from telethon import TelegramClient
from telethon.tl.types import (
    Channel, Message, MessageFwdHeader,
    PeerChannel, PeerUser, PeerChat
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, ChannelInvalidError,
    UsernameNotOccupiedError, InviteHashExpiredError
)

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredChannel:
    """Represents a discovered channel from forwards."""
    id: int
    username: Optional[str] = None
    title: Optional[str] = None
    about: Optional[str] = None
    participants_count: Optional[int] = None
    is_verified: bool = False
    is_megagroup: bool = False
    is_broadcast: bool = False

    # Discovery metadata
    discovered_from: Optional[int] = None  # Source channel ID
    forward_count: int = 0  # How many times forwarded from this channel
    first_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "username": self.username,
            "title": self.title,
            "about": self.about,
            "participants_count": self.participants_count,
            "is_verified": self.is_verified,
            "is_megagroup": self.is_megagroup,
            "is_broadcast": self.is_broadcast,
            "discovered_from": self.discovered_from,
            "forward_count": self.forward_count,
            "first_seen": self.first_seen.isoformat()
        }


@dataclass
class ChannelEdge:
    """Represents a forward relationship between channels."""
    source_id: int
    target_id: int
    forward_count: int = 0
    first_forward: Optional[datetime] = None
    last_forward: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "weight": self.forward_count,
            "first_forward": self.first_forward.isoformat() if self.first_forward else None,
            "last_forward": self.last_forward.isoformat() if self.last_forward else None
        }


class ChannelDiscovery:
    """
    Discovers related channels using the snowballing method.

    Based on PLOS ONE paper methodology:
    1. Start with seed channel(s)
    2. Scrape messages and extract forward sources
    3. Resolve forward sources to channel info
    4. Optionally recurse into discovered channels
    5. Build network graph of relationships
    """

    def __init__(self, client: TelegramClient):
        self.client = client

        # Discovered channels: id -> DiscoveredChannel
        self._channels: Dict[int, DiscoveredChannel] = {}

        # Forward relationships: (source_id, target_id) -> ChannelEdge
        self._edges: Dict[tuple, ChannelEdge] = {}

        # Tracking
        self._visited: Set[int] = set()
        self._queue: List[int] = []
        self._stats = {
            "channels_discovered": 0,
            "forwards_analyzed": 0,
            "errors": 0
        }

    async def discover_from_channel(
        self,
        channel: Any,
        message_limit: int = 1000,
        resolve_channels: bool = True,
        progress_callback: Optional[callable] = None
    ) -> List[DiscoveredChannel]:
        """
        Discover channels from forwards in a single channel.

        Args:
            channel: Channel username or ID
            message_limit: Max messages to analyze
            resolve_channels: Whether to fetch full channel info
            progress_callback: Called with (processed, total)

        Returns:
            List of discovered channels
        """
        try:
            entity = await self.client.get_entity(channel)
            source_id = entity.id

            # Add source channel
            if source_id not in self._channels:
                self._channels[source_id] = DiscoveredChannel(
                    id=source_id,
                    username=getattr(entity, 'username', None),
                    title=getattr(entity, 'title', str(source_id)),
                    is_megagroup=getattr(entity, 'megagroup', False),
                    is_broadcast=getattr(entity, 'broadcast', False)
                )

            self._visited.add(source_id)

            # Track forward sources
            forward_sources: Dict[int, int] = defaultdict(int)  # channel_id -> count
            processed = 0

            async for message in self.client.iter_messages(entity, limit=message_limit):
                processed += 1

                if message.fwd_from:
                    fwd_channel_id = self._extract_forward_source(message.fwd_from)

                    if fwd_channel_id and fwd_channel_id != source_id:
                        forward_sources[fwd_channel_id] += 1
                        self._stats["forwards_analyzed"] += 1

                        # Update edge
                        edge_key = (fwd_channel_id, source_id)
                        if edge_key not in self._edges:
                            self._edges[edge_key] = ChannelEdge(
                                source_id=fwd_channel_id,
                                target_id=source_id,
                                first_forward=message.date
                            )
                        self._edges[edge_key].forward_count += 1
                        self._edges[edge_key].last_forward = message.date

                if progress_callback and processed % 100 == 0:
                    progress_callback(processed, message_limit)

            # Resolve discovered channels
            discovered = []
            for channel_id, count in forward_sources.items():
                if channel_id not in self._channels:
                    disc_channel = DiscoveredChannel(
                        id=channel_id,
                        discovered_from=source_id,
                        forward_count=count
                    )

                    if resolve_channels:
                        try:
                            await self._resolve_channel_info(disc_channel)
                        except Exception as e:
                            logger.debug(f"Could not resolve channel {channel_id}: {e}")

                    self._channels[channel_id] = disc_channel
                    self._stats["channels_discovered"] += 1
                    discovered.append(disc_channel)
                else:
                    # Update forward count
                    self._channels[channel_id].forward_count += count

            logger.info(
                f"Discovered {len(discovered)} channels from {entity.title or source_id} "
                f"({processed} messages analyzed)"
            )

            return discovered

        except (ChannelPrivateError, ChannelInvalidError) as e:
            logger.error(f"Cannot access channel {channel}: {e}")
            self._stats["errors"] += 1
            return []

    async def snowball(
        self,
        seed_channels: List[Any],
        depth: int = 1,
        message_limit: int = 500,
        max_channels: int = 100,
        min_forward_count: int = 3,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Perform snowball sampling starting from seed channels.

        Args:
            seed_channels: Starting channel(s) username or ID
            depth: How many levels to recurse (1 = only seed channels)
            message_limit: Messages to analyze per channel
            max_channels: Stop after discovering this many channels
            min_forward_count: Minimum forwards to include channel
            progress_callback: Called with status updates

        Returns:
            Discovery results with channels and edges
        """
        # Initialize queue with seeds
        for seed in seed_channels:
            try:
                entity = await self.client.get_entity(seed)
                self._queue.append(entity.id)
            except Exception as e:
                logger.error(f"Could not resolve seed channel {seed}: {e}")

        current_depth = 0

        while self._queue and current_depth < depth:
            current_depth += 1
            current_level = list(self._queue)
            self._queue = []

            logger.info(f"Snowball depth {current_depth}: processing {len(current_level)} channels")

            for channel_id in current_level:
                if channel_id in self._visited:
                    continue

                if len(self._channels) >= max_channels:
                    logger.info(f"Reached max channels limit ({max_channels})")
                    break

                try:
                    discovered = await self.discover_from_channel(
                        channel_id,
                        message_limit=message_limit,
                        resolve_channels=True
                    )

                    # Add discovered channels to queue for next depth
                    for ch in discovered:
                        if ch.forward_count >= min_forward_count and ch.id not in self._visited:
                            self._queue.append(ch.id)

                    # Rate limiting
                    await asyncio.sleep(2)

                except FloodWaitError as e:
                    logger.warning(f"FloodWait: sleeping {e.seconds}s")
                    await asyncio.sleep(e.seconds + 5)

                except Exception as e:
                    logger.error(f"Error processing channel {channel_id}: {e}")
                    self._stats["errors"] += 1

            if progress_callback:
                progress_callback(current_depth, depth, len(self._channels))

        return self.get_results()

    def _extract_forward_source(self, fwd_header: MessageFwdHeader) -> Optional[int]:
        """Extract channel ID from forward header."""
        if fwd_header.from_id:
            if isinstance(fwd_header.from_id, PeerChannel):
                return fwd_header.from_id.channel_id
            elif isinstance(fwd_header.from_id, PeerChat):
                return fwd_header.from_id.chat_id

        # Legacy format
        if hasattr(fwd_header, 'channel_id') and fwd_header.channel_id:
            return fwd_header.channel_id

        return None

    async def _resolve_channel_info(self, channel: DiscoveredChannel) -> None:
        """Fetch full channel information."""
        try:
            entity = await self.client.get_entity(channel.id)

            channel.username = getattr(entity, 'username', None)
            channel.title = getattr(entity, 'title', None)
            channel.is_verified = getattr(entity, 'verified', False)
            channel.is_megagroup = getattr(entity, 'megagroup', False)
            channel.is_broadcast = getattr(entity, 'broadcast', False)

            # Get full info for participant count
            if isinstance(entity, Channel):
                try:
                    full = await self.client(GetFullChannelRequest(entity))
                    channel.participants_count = full.full_chat.participants_count
                    channel.about = full.full_chat.about
                except Exception:
                    pass

        except (UsernameNotOccupiedError, ChannelPrivateError, ChannelInvalidError):
            pass  # Channel is private or deleted

    def get_results(self) -> Dict[str, Any]:
        """Get discovery results."""
        return {
            "channels": [ch.to_dict() for ch in self._channels.values()],
            "edges": [e.to_dict() for e in self._edges.values()],
            "stats": {
                **self._stats,
                "total_channels": len(self._channels),
                "total_edges": len(self._edges),
                "visited": len(self._visited)
            }
        }

    def get_channels(self) -> List[DiscoveredChannel]:
        """Get all discovered channels."""
        return list(self._channels.values())

    def export_json(self, output_path: Path) -> Path:
        """Export results to JSON."""
        results = self.get_results()

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        logger.info(f"Exported {len(self._channels)} channels to {output_path}")
        return output_path

    def export_graphml(self, output_path: Path) -> Path:
        """Export network to GraphML format (for Gephi, etc.)."""
        import xml.etree.ElementTree as ET

        # Create GraphML structure
        graphml = ET.Element('graphml')
        graphml.set('xmlns', 'http://graphml.graphdrawing.org/xmlns')

        # Define attributes
        for attr_name, attr_type in [
            ('title', 'string'),
            ('username', 'string'),
            ('participants', 'int'),
            ('forward_count', 'int')
        ]:
            key = ET.SubElement(graphml, 'key')
            key.set('id', attr_name)
            key.set('for', 'node')
            key.set('attr.name', attr_name)
            key.set('attr.type', attr_type)

        # Edge weight
        key = ET.SubElement(graphml, 'key')
        key.set('id', 'weight')
        key.set('for', 'edge')
        key.set('attr.name', 'weight')
        key.set('attr.type', 'int')

        # Create graph
        graph = ET.SubElement(graphml, 'graph')
        graph.set('id', 'telegram_network')
        graph.set('edgedefault', 'directed')

        # Add nodes
        for channel in self._channels.values():
            node = ET.SubElement(graph, 'node')
            node.set('id', str(channel.id))

            if channel.title:
                data = ET.SubElement(node, 'data')
                data.set('key', 'title')
                data.text = channel.title

            if channel.username:
                data = ET.SubElement(node, 'data')
                data.set('key', 'username')
                data.text = channel.username

            if channel.participants_count:
                data = ET.SubElement(node, 'data')
                data.set('key', 'participants')
                data.text = str(channel.participants_count)

            data = ET.SubElement(node, 'data')
            data.set('key', 'forward_count')
            data.text = str(channel.forward_count)

        # Add edges
        for edge in self._edges.values():
            edge_elem = ET.SubElement(graph, 'edge')
            edge_elem.set('source', str(edge.source_id))
            edge_elem.set('target', str(edge.target_id))

            data = ET.SubElement(edge_elem, 'data')
            data.set('key', 'weight')
            data.text = str(edge.forward_count)

        # Write file
        tree = ET.ElementTree(graphml)
        tree.write(output_path, encoding='utf-8', xml_declaration=True)

        logger.info(f"Exported network to {output_path}")
        return output_path

    def export_gexf(self, output_path: Path) -> Path:
        """Export network to GEXF format (for Gephi)."""
        import xml.etree.ElementTree as ET

        # Create GEXF structure
        gexf = ET.Element('gexf')
        gexf.set('xmlns', 'http://www.gexf.net/1.2draft')
        gexf.set('version', '1.2')

        # Meta
        meta = ET.SubElement(gexf, 'meta')
        creator = ET.SubElement(meta, 'creator')
        creator.text = 'TScrape'
        desc = ET.SubElement(meta, 'description')
        desc.text = 'Telegram channel forward network'

        # Graph
        graph = ET.SubElement(gexf, 'graph')
        graph.set('defaultedgetype', 'directed')
        graph.set('mode', 'static')

        # Node attributes
        node_attrs = ET.SubElement(graph, 'attributes')
        node_attrs.set('class', 'node')

        for i, (attr_name, attr_type) in enumerate([
            ('title', 'string'),
            ('username', 'string'),
            ('participants', 'integer'),
            ('forward_count', 'integer')
        ]):
            attr = ET.SubElement(node_attrs, 'attribute')
            attr.set('id', str(i))
            attr.set('title', attr_name)
            attr.set('type', attr_type)

        # Nodes
        nodes = ET.SubElement(graph, 'nodes')
        for channel in self._channels.values():
            node = ET.SubElement(nodes, 'node')
            node.set('id', str(channel.id))
            node.set('label', channel.title or channel.username or str(channel.id))

            attvalues = ET.SubElement(node, 'attvalues')

            if channel.title:
                av = ET.SubElement(attvalues, 'attvalue')
                av.set('for', '0')
                av.set('value', channel.title)

            if channel.username:
                av = ET.SubElement(attvalues, 'attvalue')
                av.set('for', '1')
                av.set('value', channel.username)

            if channel.participants_count:
                av = ET.SubElement(attvalues, 'attvalue')
                av.set('for', '2')
                av.set('value', str(channel.participants_count))

            av = ET.SubElement(attvalues, 'attvalue')
            av.set('for', '3')
            av.set('value', str(channel.forward_count))

        # Edges
        edges = ET.SubElement(graph, 'edges')
        for i, edge in enumerate(self._edges.values()):
            edge_elem = ET.SubElement(edges, 'edge')
            edge_elem.set('id', str(i))
            edge_elem.set('source', str(edge.source_id))
            edge_elem.set('target', str(edge.target_id))
            edge_elem.set('weight', str(edge.forward_count))

        # Write file
        tree = ET.ElementTree(gexf)
        tree.write(output_path, encoding='utf-8', xml_declaration=True)

        logger.info(f"Exported network to {output_path}")
        return output_path

    def get_stats(self) -> Dict[str, int]:
        """Get discovery statistics."""
        return {
            **self._stats,
            "total_channels": len(self._channels),
            "total_edges": len(self._edges),
            "visited": len(self._visited)
        }
