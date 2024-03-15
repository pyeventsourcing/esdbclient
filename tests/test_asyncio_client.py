# -*- coding: utf-8 -*-
import asyncio
import sys
from typing import Optional

from esdbclient.streams import AsyncioCatchupSubscription
from tests.test_client import TimedTestCase, get_ca_certificate, random_data

if sys.version_info[0:2] > (3, 7):
    from unittest import IsolatedAsyncioTestCase
else:
    from async_case import IsolatedAsyncioTestCase

from uuid import uuid4

from esdbclient import NewEvent, StreamState
from esdbclient.asyncio_client import (
    AsyncioEventStoreDBClient,
    _AsyncioEventStoreDBClient,
)
from esdbclient.exceptions import (
    DiscoveryFailed,
    FollowerNotFound,
    GrpcDeadlineExceeded,
    NodeIsNotLeader,
    NotFound,
    ReadOnlyReplicaNotFound,
    ServiceUnavailable,
    StreamIsDeleted,
    WrongCurrentVersion,
)


class TestAsyncioEventStoreDBClient(TimedTestCase, IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = await AsyncioEventStoreDBClient("esdb://localhost:2113?Tls=False")
        self._reader: Optional[_AsyncioEventStoreDBClient] = None
        self._writer: Optional[_AsyncioEventStoreDBClient] = None

    @property
    def reader(self) -> _AsyncioEventStoreDBClient:
        assert self._reader is not None
        return self._reader

    @property
    def writer(self) -> _AsyncioEventStoreDBClient:
        assert self._writer is not None
        return self._writer

    async def setup_reader(self) -> None:
        self._reader = await AsyncioEventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2110,localhost:2110?NodePreference=follower",
            root_certificates=get_ca_certificate(),
        )

    async def setup_writer(self) -> None:
        self._writer = await AsyncioEventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2110,localhost:2110?NodePreference=leader",
            root_certificates=get_ca_certificate(),
        )

    async def asyncTearDown(self) -> None:
        await self.client.close()
        if self._reader is not None:
            await self._reader.close()
        if self._writer is not None:
            await self._writer.close()

    async def test_esdb_scheme_discovery_single_node_cluster(self) -> None:
        await AsyncioEventStoreDBClient(
            "esdb://localhost:2113,localhost:2113?Tls=False"
            "&GossipTimeout=1&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )

    async def test_esdb_discover_scheme_raises_discovery_failed(self) -> None:
        with self.assertRaises(DiscoveryFailed) as cm:
            await AsyncioEventStoreDBClient(
                "esdb+discover://example.com?Tls=False"
                "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
            )
        self.assertIn(":2113", str(cm.exception))
        self.assertNotIn(":9898", str(cm.exception))

        with self.assertRaises(DiscoveryFailed) as cm:
            await AsyncioEventStoreDBClient(
                "esdb+discover://example.com:9898?Tls=False"
                "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
            )
        self.assertNotIn(":2113", str(cm.exception))
        self.assertIn(":9898", str(cm.exception))

    async def test_sometimes_reconnnects_to_selected_node_after_discovery(self) -> None:
        root_certificates = get_ca_certificate()
        await AsyncioEventStoreDBClient(
            "esdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2110?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await AsyncioEventStoreDBClient(
            "esdb://admin:changeit@127.0.0.1:2111,127.0.0.1:2111?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await AsyncioEventStoreDBClient(
            "esdb://admin:changeit@127.0.0.1:2112,127.0.0.1:2112?NodePreference=leader",
            root_certificates=root_certificates,
        )

    async def test_node_preference_random(self) -> None:
        await AsyncioEventStoreDBClient(
            "esdb://localhost:2113,localhost:2113?Tls=False&NodePreference=random"
        )

    async def test_raises_follower_not_found(self) -> None:
        with self.assertRaises(FollowerNotFound):
            await AsyncioEventStoreDBClient(
                "esdb://localhost:2113,localhost:2113?Tls=False&NodePreference=follower"
            )

    async def test_raises_read_only_replica_not_found(self) -> None:
        with self.assertRaises(ReadOnlyReplicaNotFound):
            await AsyncioEventStoreDBClient(
                "esdb://localhost:2113,localhost:2113?Tls=False&NodePreference=readonlyreplica"
            )

    async def test_constructor_with_tls_true_but_no_root_certificates(self) -> None:
        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        uri = f"esdb://admin:changeit@localhost:2114?{qs}"
        with self.assertRaises(ValueError) as cm:
            await AsyncioEventStoreDBClient(uri)
        self.assertIn("Root certificate(s) are required", str(cm.exception))

    # async def test_constructor_with_tls_true_but_no_root_certificates(self) -> None:
    #     qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
    #     uri = f"esdb://admin:changeit@localhost:2114?{qs}"
    #     try:
    #         await AsyncioEventStoreDBClient(uri)
    #     except DiscoveryFailed:
    #         tb = traceback.format_exc()
    #         self.assertIn("Ssl handshake failed", tb)
    #         self.assertIn("routines:OPENSSL_internal:CERTIFICATE_VERIFY_FAILED", tb)
    #     else:
    #         self.fail("Didn't raise DiscoveryFailed exception")

    async def test_username_and_password_required_for_secure_connection(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await AsyncioEventStoreDBClient("esdb://localhost:2114")
        self.assertIn("Username and password are required", cm.exception.args[0])

    async def test_append_events_and_get_stream(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1, event2],
            current_version=StreamState.NO_STREAM,
        )

        # Read stream events.
        events = await self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

    async def test_append_events_and_read_all(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event2],
            current_version=StreamState.NO_STREAM,
        )

        # Read all events.
        events_iter = await self.client.read_all()
        event_ids = [e.id async for e in events_iter]
        self.assertIn(event1.id, event_ids)
        self.assertIn(event2.id, event_ids)

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
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
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
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

    async def test_append_events_raises_service_unavailable(self) -> None:
        await self.client._connection.close()
        self.client.connection_spec._targets = ["localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(ServiceUnavailable):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_append_events_raises_discovery_failed(self) -> None:
        await self.client._connection.close()
        self.client.connection_spec._targets = ["localhost:2222", "localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(DiscoveryFailed):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_append_events_raises_node_is_not_leader(self) -> None:
        await self.setup_reader()
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(NodeIsNotLeader):
            await self.reader.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_append_events_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )
        await self.client.delete_stream(stream_name1, current_version=0)

        await self.client.tombstone_stream(stream_name1, current_version=0)

        event2 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(StreamIsDeleted):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event2],
                current_version=StreamState.NO_STREAM,
            )

    async def test_stream_append_to_stream(self) -> None:
        # This method exists to match other language clients.
        stream_name = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append single event.
        commit_position1 = await self.client.append_to_stream(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=event1,
        )

        # Append sequence of events.
        commit_position2 = await self.client.append_to_stream(
            stream_name=stream_name,
            current_version=0,
            events=[event2, event3],
        )

        # Check commit positions are returned.
        events = [
            e
            async for e in await self.client.read_all(commit_position=commit_position1)
        ]
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].commit_position, commit_position1)
        self.assertEqual(events[2].commit_position, commit_position2)

    async def test_get_stream_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )
        await self.client.delete_stream(stream_name1, current_version=0)

        await self.client.tombstone_stream(stream_name1, current_version=0)

        with self.assertRaises(StreamIsDeleted):
            await self.client.get_stream(stream_name=stream_name1)

    async def test_append_events_reconnects_to_leader(self) -> None:
        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.reader.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

    async def test_append_events_raises_deadline_exceeded(self) -> None:
        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        stream_name1 = str(uuid4())
        events = [NewEvent(type="SomethingHappened", data=b"{}") for _ in range(1000)]
        with self.assertRaises(GrpcDeadlineExceeded):
            await self.reader.append_events(
                stream_name=stream_name1,
                events=events,
                current_version=StreamState.NO_STREAM,
                timeout=0,
            )

    async def test_get_stream_raises_not_found(self) -> None:
        with self.assertRaises(NotFound):
            await self.client.get_stream(str(uuid4()))

    async def test_get_stream_reconnects(self) -> None:
        await self.client._connection.close()
        with self.assertRaises(NotFound):
            await self.client.get_stream(str(uuid4()))

    async def test_get_stream_raises_service_unavailable(self) -> None:
        await self.client._connection.close()
        self.client.connection_spec._targets = ["localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(ServiceUnavailable):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_delete_stream_raises_stream_not_found(self) -> None:
        stream_name1 = str(uuid4())

        with self.assertRaises(NotFound):
            await self.client.delete_stream(
                stream_name1, current_version=StreamState.EXISTS
            )

    async def test_delete_stream_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        with self.assertRaises(WrongCurrentVersion):
            await self.client.delete_stream(stream_name1, current_version=10)

    async def test_delete_stream_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )
        await self.client.tombstone_stream(stream_name1, current_version=0)

        with self.assertRaises(StreamIsDeleted):
            await self.client.delete_stream(stream_name1, current_version=0)

    async def test_delete_stream_reconnects_to_leader(self) -> None:
        await self.setup_writer()

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.writer.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        await self.reader.delete_stream(stream_name1, current_version=0)

    async def test_tombstone_stream_raises_stream_not_found(self) -> None:
        stream_name1 = str(uuid4())

        with self.assertRaises(NotFound):
            await self.client.tombstone_stream(
                stream_name1, current_version=StreamState.EXISTS
            )

    async def test_tombstone_stream_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        with self.assertRaises(WrongCurrentVersion):
            await self.client.tombstone_stream(stream_name1, current_version=10)

    async def test_tombstone_stream_raises_stream_is_deleted(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )
        await self.client.tombstone_stream(stream_name1, current_version=0)

        with self.assertRaises(StreamIsDeleted):
            await self.client.tombstone_stream(stream_name1, current_version=0)

    async def test_tombstone_stream_reconnects_to_leader(self) -> None:
        await self.setup_writer()

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.writer.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        await self.setup_reader()
        self.reader.connection_spec.options._NodePreference = "leader"

        await self.reader.tombstone_stream(stream_name1, current_version=0)

    async def test_subscribe_to_all(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event2],
            current_version=StreamState.NO_STREAM,
        )

        # Subscribe all events.
        catchup_subscription = await self.client.subscribe_to_all()
        events = []
        async for event in catchup_subscription:
            events.append(event)
            if event.id == event2.id:
                catchup_subscription.stop()
        self.assertEqual(events[-2].id, event1.id)
        self.assertEqual(events[-1].id, event2.id)

    async def test_subscribe_to_all_with_gather(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event2],
            current_version=StreamState.NO_STREAM,
        )

        class Worker:
            def __init__(self, client: _AsyncioEventStoreDBClient) -> None:
                self.client = client

            async def run(self) -> None:
                catchup_subscription = await self.client.subscribe_to_all()
                events = []
                async for event in catchup_subscription:
                    events.append(event)
                    if event.id == event2.id:
                        catchup_subscription.stop()

        await asyncio.gather(Worker(self.client).run(), Worker(self.client).run())

    async def test_subscribe_to_all_reconnects(self) -> None:
        # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
        await self.client._connection.close()
        self.client._connection = self.client._construct_esdb_connection(
            "localhost:2222"
        )

        catchup_subscription = await self.client.subscribe_to_all()
        self.assertIsInstance(catchup_subscription, AsyncioCatchupSubscription)

    # async def test_subscribe_to_all_raises_discovery_failed(self) -> None:
    #     await self.client._connection.close()
    #     # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
    #     await self.client._connection.close()
    #     self.client._connection = self.client._construct_esdb_connection("localhost:2222")
    #
    #     await self.client.subscribe_to_all()
    #     # with self.assertRaises(ServiceUnavailable):
