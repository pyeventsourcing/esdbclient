# -*- coding: utf-8 -*-
import sys
from typing import Optional

from tests.test_client import get_ca_certificate

if sys.version_info[0:2] > (3, 7):
    from unittest import IsolatedAsyncioTestCase
else:
    from async_case import IsolatedAsyncioTestCase

from uuid import uuid4

from esdbclient import NewEvent
from esdbclient.asyncio_client import AsyncioESDBClient, _AsyncioESDBClient
from esdbclient.exceptions import (
    DeadlineExceeded,
    DiscoveryFailed,
    DNSError,
    FollowerNotFound,
    GossipSeedError,
    NodeIsNotLeader,
    NotFound,
    ReadOnlyReplicaNotFound,
    StreamIsDeleted,
    WrongCurrentVersion,
)


class TestAsyncioESDBClient(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await AsyncioESDBClient("esdb://localhost:2114?Tls=False")
        self._reader: Optional[_AsyncioESDBClient] = None
        self._writer: Optional[_AsyncioESDBClient] = None

    @property
    def reader(self) -> _AsyncioESDBClient:
        assert self._reader is not None
        return self._reader

    @property
    def writer(self) -> _AsyncioESDBClient:
        assert self._writer is not None
        return self._writer

    async def setup_reader(self) -> None:
        self._reader = await AsyncioESDBClient(
            uri="esdb://admin:changeit@localhost:2111?NodePreference=follower",
            root_certificates=get_ca_certificate(),
        )

    async def setup_writer(self) -> None:
        self._writer = await AsyncioESDBClient(
            uri="esdb://admin:changeit@localhost:2111?NodePreference=leader",
            root_certificates=get_ca_certificate(),
        )

    async def asyncTearDown(self) -> None:
        await self.client.close()
        if self._reader is not None:
            await self._reader.close()
        if self._writer is not None:
            await self._writer.close()

    async def test_raises_dns_error(self) -> None:
        with self.assertRaises(DNSError):
            await AsyncioESDBClient("esdb+discover://xxxxxxxxxxxxxx?Tls=false")

    async def test_calls_dns(self) -> None:
        with self.assertRaises(DiscoveryFailed):
            await AsyncioESDBClient(
                "esdb+discover://example.com?Tls=False"
                "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
            )

    async def test_raises_gossip_seed_error(self) -> None:
        with self.assertRaises(GossipSeedError):
            await AsyncioESDBClient("esdb://")

    async def test_sometimes_reconnnects_to_selected_node_after_discovery(self) -> None:
        root_certificates = get_ca_certificate()
        await AsyncioESDBClient(
            "esdb://admin:changeit@127.0.0.1:2111?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await AsyncioESDBClient(
            "esdb://admin:changeit@127.0.0.1:2112?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await AsyncioESDBClient(
            "esdb://admin:changeit@127.0.0.1:2113?NodePreference=leader",
            root_certificates=root_certificates,
        )

    async def test_node_preference_random(self) -> None:
        await AsyncioESDBClient("esdb://localhost:2114?Tls=False&NodePreference=random")

    async def test_raises_follower_not_found(self) -> None:
        with self.assertRaises(FollowerNotFound):
            await AsyncioESDBClient(
                "esdb://localhost:2114?Tls=False&NodePreference=follower"
            )

    async def test_raises_read_only_replica_not_found(self) -> None:
        with self.assertRaises(ReadOnlyReplicaNotFound):
            await AsyncioESDBClient(
                "esdb://localhost:2114?Tls=False&NodePreference=readonlyreplica"
            )

    async def test_root_certificates_required_for_secure_connection(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await AsyncioESDBClient("esdb://localhost:2115")
        self.assertIn(
            "root_certificates is required for secure connection", cm.exception.args[0]
        )

    async def test_append_events_and_read_stream_events(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1, event2], current_version=None
        )

        # Read stream events.
        events = await self.client.read_stream_events(stream_name1)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

    async def test_append_events_and_read_all_events(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2, events=[event2], current_version=None
        )

        # Read all events.
        events_iter = await self.client.read_all_events()
        event_ids = [e.id async for e in events_iter]
        self.assertIn(event1.id, event_ids)
        self.assertIn(event2.id, event_ids)

    async def test_append_events_and_subscribe_all_events(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2, events=[event2], current_version=None
        )

        # Subscribe all events.
        events_iter = await self.client.subscribe_all_events()
        events = []
        async for event in events_iter:
            events.append(event)
            if event.id == event2.id:
                break
        self.assertEqual(events[-2].id, event1.id)
        self.assertEqual(events[-1].id, event2.id)

    async def test_append_events_raises_not_found(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(NotFound):
            await self.client.append_events(
                stream_name=stream_name1, events=[event1], current_version=10
            )

    async def test_append_events_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        event2 = NewEvent(type="OrderUpdated", data=b"{}")
        with self.assertRaises(WrongCurrentVersion):
            await self.client.append_events(
                stream_name=stream_name1, events=[event2], current_version=10
            )

    async def test_append_events_reconnects_closed_connection(self) -> None:
        await self.client._connection.close()
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

    async def test_append_events_raises_discovery_failed(self) -> None:
        await self.client._connection.close()
        self.client.connection_spec._targets = ["localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(DiscoveryFailed):
            await self.client.append_events(
                stream_name=stream_name1, events=[event1], current_version=None
            )

    async def test_append_events_raises_node_is_not_leader(self) -> None:
        await self.setup_reader()
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(NodeIsNotLeader):
            await self.reader.append_events(
                stream_name=stream_name1, events=[event1], current_version=None
            )

    async def test_append_events_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )
        await self.client.delete_stream(stream_name1, current_version=0)

        await self.client.tombstone_stream(stream_name1, current_version=0)

        event2 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(StreamIsDeleted):
            await self.client.append_events(
                stream_name=stream_name1, events=[event2], current_version=None
            )

    async def test_read_stream_events_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )
        await self.client.delete_stream(stream_name1, current_version=0)

        await self.client.tombstone_stream(stream_name1, current_version=0)

        with self.assertRaises(StreamIsDeleted):
            await self.client.read_stream_events(stream_name=stream_name1)

    async def test_append_events_reconnects_to_leader(self) -> None:
        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.reader.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

    async def test_append_events_raises_deadline_exceeded(self) -> None:
        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        stream_name1 = str(uuid4())
        events = [NewEvent(type="SomethingHappened", data=b"{}") for _ in range(1000)]
        with self.assertRaises(DeadlineExceeded):
            await self.reader.append_events(
                stream_name=stream_name1,
                events=events,
                current_version=None,
                timeout=0,
            )

    async def test_read_stream_events_raises_not_found(self) -> None:
        with self.assertRaises(NotFound):
            await self.client.read_stream_events(str(uuid4()))

    async def test_read_stream_events_reconnects(self) -> None:
        await self.client._connection.close()
        with self.assertRaises(NotFound):
            await self.client.read_stream_events(str(uuid4()))

    async def test_read_stream_events_raises_discovery_failed(self) -> None:
        await self.client._connection.close()
        self.client.connection_spec._targets = ["localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(DiscoveryFailed):
            await self.client.append_events(
                stream_name=stream_name1, events=[event1], current_version=None
            )

    async def test_delete_stream_raises_stream_not_found(self) -> None:
        stream_name1 = str(uuid4())

        with self.assertRaises(NotFound):
            await self.client.delete_stream(stream_name1, current_version=None)

    async def test_delete_stream_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        with self.assertRaises(WrongCurrentVersion):
            await self.client.delete_stream(stream_name1, current_version=10)

    async def test_delete_stream_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )
        await self.client.tombstone_stream(stream_name1, current_version=0)

        with self.assertRaises(StreamIsDeleted):
            await self.client.delete_stream(stream_name1, current_version=0)

    async def test_delete_stream_reconnects_to_leader(self) -> None:
        await self.setup_writer()

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.writer.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        await self.reader.delete_stream(stream_name1, current_version=0)

    async def test_tombstone_stream_raises_stream_not_found(self) -> None:
        stream_name1 = str(uuid4())

        with self.assertRaises(NotFound):
            await self.client.tombstone_stream(stream_name1, current_version=None)

    async def test_tombstone_stream_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        with self.assertRaises(WrongCurrentVersion):
            await self.client.tombstone_stream(stream_name1, current_version=10)

    async def test_tombstone_stream_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )
        await self.client.tombstone_stream(stream_name1, current_version=0)

        with self.assertRaises(StreamIsDeleted):
            await self.client.tombstone_stream(stream_name1, current_version=0)

    async def test_tombstone_stream_reconnects_to_leader(self) -> None:
        await self.setup_writer()

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.writer.append_events(
            stream_name=stream_name1, events=[event1], current_version=None
        )

        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        await self.reader.tombstone_stream(stream_name1, current_version=0)

    async def test_subscribe_all_events_raises_service_unavailable(self) -> None:
        await self.client._connection.close()
        # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
        await self.client._connection.close()
        self.client._connection = self.client._construct_connection("localhost:2222")

        with self.assertRaises(NotFound):
            await self.client.read_stream_events(str(uuid4()))
