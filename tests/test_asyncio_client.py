# -*- coding: utf-8 -*-
import asyncio
import json
import os
from tempfile import NamedTemporaryFile
from typing import Optional, cast
from unittest import IsolatedAsyncioTestCase, skipIf
from uuid import uuid4

from esdbclient import (
    AsyncioEventStoreDBClient,
    AsyncPersistentSubscription,
    Checkpoint,
    NewEvent,
    StreamState,
)
from esdbclient.asyncio_client import (
    AsyncEventStoreDBClient,
    _AsyncioEventStoreDBClient,
)
from esdbclient.common import (
    DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
    DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
)
from esdbclient.events import CaughtUp
from esdbclient.exceptions import (
    AlreadyExists,
    DeadlineExceeded,
    DiscoveryFailed,
    ExceptionIteratingRequests,
    ExceptionThrownByHandler,
    FollowerNotFound,
    GrpcDeadlineExceeded,
    NodeIsNotLeader,
    NotFound,
    OperationFailed,
    ProgrammingError,
    ReadOnlyReplicaNotFound,
    ServiceUnavailable,
    SSLError,
    StreamIsDeleted,
    WrongCurrentVersion,
)
from esdbclient.persistent import AsyncSubscriptionReadReqs
from esdbclient.streams import AsyncCatchupSubscription
from tests.test_client import (
    EVENTSTORE_DOCKER_IMAGE,
    PROJECTION_QUERY_TEMPLATE1,
    TimedTestCase,
    get_ca_certificate,
    get_server_certificate,
    random_data,
)


class TestAsyncEventStoreDBClient(TimedTestCase, IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = AsyncEventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2114",
            root_certificates=get_server_certificate("localhost:2114"),
        )
        await self.client.connect()
        self._reader: Optional[AsyncEventStoreDBClient] = None
        self._writer: Optional[AsyncEventStoreDBClient] = None

    @property
    def reader(self) -> AsyncEventStoreDBClient:
        assert self._reader is not None
        return self._reader

    @property
    def writer(self) -> AsyncEventStoreDBClient:
        assert self._writer is not None
        return self._writer

    async def setup_reader(self) -> None:
        self._reader = AsyncEventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2110,localhost:2110?NodePreference=follower",
            root_certificates=get_ca_certificate(),
        )
        await self._reader.connect()

    async def setup_writer(self) -> None:
        self._writer = AsyncEventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2110,localhost:2110?NodePreference=leader",
            root_certificates=get_ca_certificate(),
        )
        await self._writer.connect()

    async def asyncTearDown(self) -> None:
        try:
            if hasattr(self, "client") and not self.client.is_closed:
                for subscription in await self.client.list_subscriptions():
                    await self.client.delete_subscription(
                        group_name=subscription.group_name,
                        stream_name=(
                            None
                            if subscription.event_source == "$all"
                            else subscription.event_source
                        ),
                    )
            await self.client.close()
            del self.client

            if self._reader is not None:
                await self._reader.close()
                del self._reader

            if self._writer is not None and self._writer.is_closed:
                for subscription in await self._writer.list_subscriptions():
                    await self.client.delete_subscription(
                        group_name=subscription.group_name,
                        stream_name=(
                            None
                            if subscription.event_source == "$all"
                            else subscription.event_source
                        ),
                    )
                await self._writer.close()
                del self._writer
        except (ServiceUnavailable, DiscoveryFailed):
            pass
        finally:
            await super().asyncTearDown()

    async def test_esdb_scheme_discovery_single_node_cluster(self) -> None:
        client = AsyncEventStoreDBClient(
            "esdb://localhost:2113,localhost:2113?Tls=False"
            "&GossipTimeout=1&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )
        await client.connect()
        self.assertEqual("localhost:2113", client.connection_target)

    async def test_esdb_discover_scheme_raises_discovery_failed(self) -> None:
        client = AsyncEventStoreDBClient(
            "esdb+discover://example.com?Tls=False"
            "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )
        with self.assertRaises(DiscoveryFailed) as cm:
            await client.connect()
        self.assertIn(":2113", str(cm.exception))
        self.assertNotIn(":9898", str(cm.exception))

        client = AsyncEventStoreDBClient(
            "esdb+discover://example.com:9898?Tls=False"
            "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )
        with self.assertRaises(DiscoveryFailed) as cm:
            await client.connect()
        self.assertNotIn(":2113", str(cm.exception))
        self.assertIn(":9898", str(cm.exception))

    async def test_sometimes_reconnnects_to_selected_node_after_discovery(self) -> None:
        root_certificates = get_ca_certificate()
        client = AsyncEventStoreDBClient(
            "esdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2110?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await client.connect()
        client = AsyncEventStoreDBClient(
            "esdb://admin:changeit@127.0.0.1:2111,127.0.0.1:2111?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await client.connect()
        client = AsyncEventStoreDBClient(
            "esdb://admin:changeit@127.0.0.1:2112,127.0.0.1:2112?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await client.connect()

    async def test_node_preference_random(self) -> None:
        client = AsyncEventStoreDBClient(
            "esdb://localhost:2113,localhost:2113?Tls=False&NodePreference=random"
        )
        await client.connect()

    async def test_raises_follower_not_found(self) -> None:
        client = AsyncEventStoreDBClient(
            "esdb://localhost:2113,localhost:2113?Tls=False&NodePreference=follower"
        )
        with self.assertRaises(FollowerNotFound):
            await client.connect()

    async def test_raises_read_only_replica_not_found(self) -> None:
        client = AsyncEventStoreDBClient(
            "esdb://localhost:2113,localhost:2113?Tls=False&NodePreference=readonlyreplica"
        )
        with self.assertRaises(ReadOnlyReplicaNotFound):
            await client.connect()

    async def test_raises_ssl_error_with_tls_true_but_no_root_certificates(
        self,
    ) -> None:
        # NB Client can work with Tls=True without setting 'root_certificates'
        # if grpc lib can verify server cert using locally installed CA certs.
        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        uri = f"esdb://admin:changeit@localhost:2114?{qs}"
        client = AsyncEventStoreDBClient(uri)
        await client.connect()
        with self.assertRaises(SSLError):
            await client.get_commit_position()

    async def test_raises_ssl_error_with_tls_true_broken_root_certificates(
        self,
    ) -> None:
        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        uri = f"esdb://admin:changeit@localhost:2114?{qs}"
        client = AsyncEventStoreDBClient(uri, root_certificates="blah")
        await client.connect()
        with self.assertRaises(SSLError):
            await client.get_commit_position()

    async def test_raises_discovery_failed_with_tls_true_but_no_root_certificate(
        self,
    ) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111"
        uri += "?MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

        client = AsyncEventStoreDBClient(uri, root_certificates="blah")
        with self.assertRaises(DiscoveryFailed):
            await client.connect()

    async def test_username_and_password_required_for_secure_connection(self) -> None:
        with self.assertRaises(ValueError) as cm:
            AsyncEventStoreDBClient("esdb://localhost:2114")
        self.assertIn("Username and password are required", cm.exception.args[0])

    async def test_context_manager(self) -> None:
        async with self.client:
            self.assertFalse(self.client.is_closed)
        self.assertTrue(self.client.is_closed)

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

    async def test_get_commit_position(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        commit_position1 = await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        # Get commit position.
        commit_position2 = await self.client.get_commit_position()
        self.assertEqual(commit_position1, commit_position2)

        commit_position3 = await self.client.get_commit_position(filter_exclude=[".*"])
        self.assertEqual(0, commit_position3)

    async def test_get_current_version(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        current_version = await self.client.get_current_version(stream_name1)
        self.assertEqual(StreamState.NO_STREAM, current_version)

        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        # Get current version.
        current_version = await self.client.get_current_version(stream_name1)
        self.assertEqual(0, current_version)

    async def test_stream_metadata_get_and_set(self) -> None:
        stream_name = str(uuid4())

        # Append batch of new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        await self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
        )
        self.assertEqual(2, len(await self.client.get_stream(stream_name)))

        # Get stream metadata (should be empty).
        metadata, version = await self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata, {})

        # Delete stream.
        await self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)
        with self.assertRaises(NotFound):
            await self.client.get_stream(stream_name)

        # Get stream metadata (should have "$tb").
        metadata, version = await self.client.get_stream_metadata(stream_name)
        self.assertIsInstance(metadata, dict)
        self.assertIn("$tb", metadata)
        max_long = 9223372036854775807
        self.assertEqual(metadata["$tb"], max_long)

        # Set stream metadata.
        metadata["foo"] = "bar"
        await self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
            current_version=version,
        )

        # Check the metadata has "foo".
        metadata, version = await self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata["foo"], "bar")

        # For some reason "$tb" is now (most often) 2 rather than max_long.
        # Todo: Why is this?
        self.assertIn(metadata["$tb"], [2, max_long])

        # Get and set metadata for a stream that does not exist.
        stream_name = str(uuid4())
        metadata, version = await self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata, {})

        metadata["foo"] = "baz"
        await self.client.set_stream_metadata(
            stream_name=stream_name, metadata=metadata, current_version=version
        )
        metadata, version = await self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata["foo"], "baz")

        # Set ACL.
        self.assertNotIn("$acl", metadata)
        acl = {
            "$w": "$admins",
            "$r": "$all",
            "$d": "$admins",
            "$mw": "$admins",
            "$mr": "$admins",
        }
        metadata["$acl"] = acl
        await self.client.set_stream_metadata(
            stream_name, metadata=metadata, current_version=version
        )
        metadata, version = await self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata["$acl"], acl)

        with self.assertRaises(WrongCurrentVersion):
            await self.client.set_stream_metadata(
                stream_name=stream_name,
                metadata=metadata,
                current_version=10,
            )

        await self.client.tombstone_stream(stream_name, current_version=StreamState.ANY)

        # Can't get metadata after tombstoning stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            await self.client.get_stream_metadata(stream_name)

        # For some reason, we can set stream metadata, even though the stream
        # has been tombstoned, and even though we can't get stream metadata.
        # Todo: Ask ESDB team why this is?
        await self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
            current_version=1,
        )

        await self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
            current_version=StreamState.ANY,
        )

        with self.assertRaises(StreamIsDeleted):
            await self.client.get_stream_metadata(stream_name)

    async def test_append_events_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(WrongCurrentVersion):
            await self.client.append_events(
                stream_name=stream_name1, events=[event1], current_version=10
            )

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
                await catchup_subscription.stop()
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
            def __init__(self, client: AsyncEventStoreDBClient) -> None:
                self.client = client

            async def run(self) -> None:
                catchup_subscription = await self.client.subscribe_to_all()
                events = []
                async for event in catchup_subscription:
                    events.append(event)
                    if event.id == event2.id:
                        await catchup_subscription.stop()

        await asyncio.gather(Worker(self.client).run(), Worker(self.client).run())

    async def test_subscribe_to_all_reconnects(self) -> None:
        # Reconstruct connection with wrong port (to inspire UsageError).
        await self.client._connection.close()
        catchup_subscription = await self.client.subscribe_to_all()
        self.assertIsInstance(catchup_subscription, AsyncCatchupSubscription)

        # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
        self.client._connection = self.client._construct_esdb_connection(
            "localhost:22222"
        )
        catchup_subscription = await self.client.subscribe_to_all()
        self.assertIsInstance(catchup_subscription, AsyncCatchupSubscription)

    async def test_subscribe_to_all_include_checkpoints(self) -> None:
        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe excluding all events, with small window.
        subscription = await self.client.subscribe_to_all(
            filter_exclude=".*",
            include_checkpoints=True,
            window_size=1,
            checkpoint_interval_multiplier=1,
        )

        # Expect to get checkpoints.
        async for event in subscription:
            if isinstance(event, Checkpoint):
                break

    @skipIf(
        "21.10" in EVENTSTORE_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    @skipIf(
        "22.10" in EVENTSTORE_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    async def test_subscribe_to_all_include_caught_up(self) -> None:
        commit_position = await self.client.get_commit_position()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        stream_name1 = str(uuid4())
        await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )

        # Subscribe excluding all events, with small window.
        subscription = await self.client.subscribe_to_all(
            commit_position=commit_position,
            filter_exclude=".*",
            include_caught_up=True,
            timeout=10,
        )

        # Expect to get caught up message.
        async for event in subscription:
            if isinstance(event, CaughtUp):
                break

    async def test_subscribe_to_stream(self) -> None:
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

        # Subscribe to stream1.
        catchup_subscription = await self.client.subscribe_to_stream(stream_name1)
        events = []
        async for event in catchup_subscription:
            events.append(event)
            if event.id == event1.id:
                await catchup_subscription.stop()
        self.assertEqual(events[-1].id, event1.id)

        # Subscribe to stream2.
        catchup_subscription = await self.client.subscribe_to_stream(stream_name2)
        events = []
        async for event in catchup_subscription:
            events.append(event)
            if event.id == event2.id:
                await catchup_subscription.stop()
        self.assertEqual(events[-1].id, event2.id)

    async def test_subscription_to_stream_update(self) -> None:
        group_name = f"my-subscription-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"

        # Can't update subscription that doesn't exist.
        with self.assertRaises(NotFound):
            await self.client.update_subscription_to_stream(
                group_name=group_name,
                stream_name=stream_name,
            )

        # Append an event.
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}")
        await self.client.append_events(
            stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )

        # Create persistent subscription with defaults.
        await self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, False)
        self.assertEqual(info.consumer_strategy, "DispatchToSingle")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update to resolve links.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, resolve_links=True
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "DispatchToSingle")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update consumer_strategy.
        await self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            consumer_strategy="RoundRobin",
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update message_timeout.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, message_timeout=15.0
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update max_retry_count.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_retry_count=5
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update min_checkpoint_count.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, min_checkpoint_count=7
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update max_checkpoint_count.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_checkpoint_count=12
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update checkpoint_after.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, checkpoint_after=1.0
        )
        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update max_subscriber_count.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_subscriber_count=10
        )
        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update live_buffer_size.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, live_buffer_size=300
        )
        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update read_batch_size.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, read_batch_size=250
        )
        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update history_buffer_size.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, history_buffer_size=400
        )
        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, False)

        # Update extra_statistics.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, extra_statistics=True
        )
        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from end.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, from_end=True
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "-1")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from same position (the end).
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "-1")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from stream_position.
        stream_position = await self.client.get_current_version(stream_name)
        assert isinstance(stream_position, int)
        await self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            stream_position=stream_position,
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, f"{stream_position}")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from same stream_position.
        await self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, f"{stream_position}")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from start.
        await self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            from_end=False,
        )

        info = await self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

    @skipIf(
        "21.10" in EVENTSTORE_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    @skipIf(
        "22.10" in EVENTSTORE_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    async def test_subscribe_to_stream_include_caught_up(self) -> None:
        event1 = NewEvent(type="OrderCreated", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )

        # Subscribe to stream events, from the start.
        subscription = await self.client.subscribe_to_stream(
            stream_name=stream_name1,
            include_caught_up=True,
            timeout=10,
        )
        async for event in subscription:
            if isinstance(event, CaughtUp):
                break

    async def test_persistent_subscription_to_all(self) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFound):
            await self.client.get_subscription_info(group_name)

        # Create subscription.
        await self.client.create_subscription_to_all(group_name, from_end=True)

        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated1", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated2", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event2],
            current_version=StreamState.NO_STREAM,
        )

        # Read subscription - error iterating requests is propagated.
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        with self.assertRaises(ExceptionIteratingRequests):
            async for _ in persistent_subscription:
                await persistent_subscription.ack("a")  # type: ignore[arg-type]

        # Read subscription - success.
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        events = []
        async for event in persistent_subscription:
            events.append(event)
            await persistent_subscription.ack(event)
            if event.id == event2.id:
                await persistent_subscription.stop()

        self.assertEqual(len(events), 2)
        self.assertEqual(events[-2].id, event1.id)
        self.assertEqual(events[-1].id, event2.id)

        # Replay parked.
        # - append more events
        stream_name3 = str(uuid4())
        event3 = NewEvent(type="OrderCreated3", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name3,
            events=[event3],
            current_version=StreamState.NO_STREAM,
        )
        stream_name4 = str(uuid4())
        event4 = NewEvent(type="OrderCreated4", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name4,
            events=[event4],
            current_version=StreamState.NO_STREAM,
        )
        # - retry events
        events = []
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        async for event in persistent_subscription:
            events.append(event)
            if event.id in [event3.id, event4.id]:
                await persistent_subscription.nack(event, "retry")
            else:
                await persistent_subscription.ack(event)
            if event.id == event4.id:
                await persistent_subscription.stop()

        self.assertEqual(len(events), 2)
        self.assertEqual(events[-2].id, event3.id)
        self.assertEqual(events[-1].id, event4.id)

        # - park events
        events = []
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        async for event in persistent_subscription:
            events.append(event)
            if event.id in [event3.id, event4.id]:
                await persistent_subscription.nack(event, "park")
            else:
                await persistent_subscription.ack(event)
            if event.id == event4.id:
                await persistent_subscription.stop()

        self.assertEqual(len(events), 2)
        self.assertEqual(events[-2].id, event3.id)
        self.assertEqual(events[-1].id, event4.id)

        # - call replay_parked_events()
        await self.client.replay_parked_events(group_name=group_name)

        # - continue iterating over subscription
        events = []
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        async for event in persistent_subscription:
            events.append(event)
            await persistent_subscription.ack(event)
            if event.id == event4.id:
                await persistent_subscription.stop()
        self.assertEqual(len(events), 2)
        self.assertEqual(events[-2].id, event3.id)
        self.assertEqual(events[-1].id, event4.id)

        # Get subscription info.
        info = await self.client.get_subscription_info(group_name)
        self.assertEqual(info.group_name, group_name)
        self.assertFalse(info.resolve_links)

        # Update subscription.
        await self.client.update_subscription_to_all(
            group_name=group_name, resolve_links=True
        )
        info = await self.client.get_subscription_info(group_name)
        self.assertTrue(info.resolve_links)

        # List subscriptions.
        subscription_infos = await self.client.list_subscriptions()
        for subscription_info in subscription_infos:
            if subscription_info.group_name == group_name:
                break
        else:
            self.fail("Subscription not found in list")

        # Delete subscription.
        await self.client.delete_subscription(group_name=group_name)
        with self.assertRaises(NotFound):
            await self.client.read_subscription_to_all(group_name)

        subscription_infos = await self.client.list_subscriptions()
        for subscription_info in subscription_infos:
            if subscription_info.group_name == group_name:
                self.fail("Subscription found in list")

        # - raises NotFound
        with self.assertRaises(NotFound):
            await self.client.read_subscription_to_all(group_name)
        with self.assertRaises(NotFound):
            await self.client.update_subscription_to_all(group_name)
        with self.assertRaises(NotFound):
            await self.client.get_subscription_info(group_name)
        with self.assertRaises(NotFound):
            await self.client.replay_parked_events(group_name)

    async def test_subscription_to_all_update(self) -> None:
        group_name = f"my-subscription-{uuid4().hex}"

        # Can't update subscription that doesn't exist.
        with self.assertRaises(NotFound):
            # raises in get_info()
            await self.client.update_subscription_to_all(group_name=group_name)
        with self.assertRaises(NotFound):
            # raises in update()
            await self.client._connection.persistent_subscriptions.update(
                group_name=group_name,
                metadata=self.client._call_metadata,
                credentials=self.client._call_credentials,
            )

        # Create persistent subscription with defaults.
        await self.client.create_subscription_to_all(
            group_name=group_name,
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, False)
        self.assertEqual(info.consumer_strategy, "DispatchToSingle")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update to resolve links.
        await self.client.update_subscription_to_all(
            group_name=group_name, resolve_links=True
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "DispatchToSingle")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update consumer_strategy.
        await self.client.update_subscription_to_all(
            group_name=group_name, consumer_strategy="RoundRobin"
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "RoundRobin")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        await self.client.update_subscription_to_all(
            group_name=group_name, consumer_strategy="Pinned"
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(
            info.message_timeout, DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT
        )
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update message_timeout.
        await self.client.update_subscription_to_all(
            group_name=group_name, message_timeout=15.0
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(
            info.max_retry_count, DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT
        )
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update max_retry_count.
        await self.client.update_subscription_to_all(
            group_name=group_name, max_retry_count=5
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update min_checkpoint_count.
        await self.client.update_subscription_to_all(
            group_name=group_name, min_checkpoint_count=7
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update max_checkpoint_count.
        await self.client.update_subscription_to_all(
            group_name=group_name, max_checkpoint_count=12
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(
            info.checkpoint_after, DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER
        )
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update checkpoint_after.
        await self.client.update_subscription_to_all(
            group_name=group_name, checkpoint_after=1.0
        )
        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update max_subscriber_count.
        await self.client.update_subscription_to_all(
            group_name=group_name, max_subscriber_count=10
        )
        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(
            info.live_buffer_size, DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE
        )
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update live_buffer_size.
        await self.client.update_subscription_to_all(
            group_name=group_name, live_buffer_size=300
        )
        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(
            info.read_batch_size, DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE
        )
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update read_batch_size.
        await self.client.update_subscription_to_all(
            group_name=group_name, read_batch_size=250
        )
        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        # Update history_buffer_size.
        await self.client.update_subscription_to_all(
            group_name=group_name, history_buffer_size=400
        )
        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, False)

        # Update extra_statistics.
        await self.client.update_subscription_to_all(
            group_name=group_name, extra_statistics=True
        )
        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from end.
        await self.client.update_subscription_to_all(
            group_name=group_name, from_end=True
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:-1/P:-1")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from same position (the end).
        await self.client.update_subscription_to_all(group_name=group_name)

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:-1/P:-1")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from stream_position.
        commit_position = await self.client.get_commit_position()
        await self.client.update_subscription_to_all(
            group_name=group_name,
            commit_position=commit_position,
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, f"C:{commit_position}/P:{commit_position}")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from same stream_position.
        await self.client.update_subscription_to_all(
            group_name=group_name,
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, f"C:{commit_position}/P:{commit_position}")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

        # Update to run from start.
        await self.client.update_subscription_to_all(
            group_name=group_name,
            from_end=False,
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, 15.0)
        self.assertEqual(info.max_retry_count, 5)
        self.assertEqual(info.min_checkpoint_count, 7)
        self.assertEqual(info.max_checkpoint_count, 12)
        self.assertEqual(info.checkpoint_after, 1.0)
        self.assertEqual(info.max_subscriber_count, 10)
        self.assertEqual(info.live_buffer_size, 300)
        self.assertEqual(info.read_batch_size, 250)
        self.assertEqual(info.history_buffer_size, 400)
        self.assertEqual(info.extra_statistics, True)

    async def test_persistent_subscription_to_stream(self) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        stream_name1 = str(uuid4())
        stream_name2 = str(uuid4())
        with self.assertRaises(NotFound):
            await self.client.get_subscription_info(group_name, stream_name1)

        # Create subscription.
        await self.client.create_subscription_to_stream(group_name, stream_name1)
        await self.client.create_subscription_to_stream(group_name, stream_name2)

        # Append events.
        event1 = NewEvent(type="OrderCreated1", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        event2 = NewEvent(type="OrderCreated2", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event2],
            current_version=StreamState.NO_STREAM,
        )

        # Read subscription - error iterating requests is propagated.
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        with self.assertRaises(ExceptionIteratingRequests):
            async for _ in subscription:
                await subscription.ack("a")  # type: ignore[arg-type]

        # Read subscription - success.
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        events = []
        async for event in subscription:
            events.append(event)
            await subscription.ack(event)
            if event.id == event1.id:
                await subscription.stop()

        self.assertEqual(len(events), 1)
        self.assertEqual(events[-1].id, event1.id)

        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name2
        )
        events = []
        async for event in subscription:
            events.append(event)
            await subscription.ack(event)
            if event.id == event2.id:
                await subscription.stop()

        self.assertEqual(len(events), 1)
        self.assertEqual(events[-1].id, event2.id)

        # Replay parked.
        # - append more events
        event3 = NewEvent(type="OrderCreated3", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event3],
            current_version=0,
        )
        event4 = NewEvent(type="OrderCreated4", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event4],
            current_version=0,
        )
        # - retry events
        events = []
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        async for event in subscription:
            events.append(event)
            if event.id == event3.id:
                await subscription.nack(event, "retry")
                await subscription.stop()
            else:
                await subscription.ack(event)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[-1].id, event3.id)

        # - park events
        events = []
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        async for event in subscription:
            events.append(event)
            if event.id == event3.id:
                await subscription.nack(event, "park")
                await subscription.stop()
            else:
                await subscription.ack(event)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[-1].id, event3.id)

        # - call replay_parked_events()
        await self.client.replay_parked_events(group_name, stream_name1)

        # - continue iterating over subscription
        events = []
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        async for event in subscription:
            events.append(event)
            await subscription.ack(event)
            if event.id == event3.id:
                await subscription.stop()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[-1].id, event3.id)

        # Get subscription info.
        info = await self.client.get_subscription_info(group_name, stream_name1)
        self.assertEqual(info.group_name, group_name)
        self.assertEqual(info.event_source, stream_name1)
        self.assertFalse(info.resolve_links)

        # Update subscription.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name1, resolve_links=True
        )
        info = await self.client.get_subscription_info(group_name, stream_name1)
        self.assertTrue(info.resolve_links)

        # List subscriptions.
        subscription_infos = await self.client.list_subscriptions()
        for subscription_info in subscription_infos:
            if (
                subscription_info.group_name == group_name
                and subscription_info.event_source == stream_name1
            ):
                break
        else:
            self.fail("Subscription not found in list")

        # Delete subscription.
        await self.client.delete_subscription(group_name, stream_name1)

        subscription_infos = await self.client.list_subscriptions()
        for subscription_info in subscription_infos:
            if (
                subscription_info.group_name == group_name
                and subscription_info.event_source == stream_name1
            ):
                self.fail("Subscription found in list")

        # - raises NotFound
        with self.assertRaises(NotFound):
            await self.client.read_subscription_to_stream(group_name, stream_name1)
        with self.assertRaises(NotFound):
            await self.client.update_subscription_to_stream(group_name, stream_name1)
        with self.assertRaises(NotFound):
            await self.client.get_subscription_info(group_name, stream_name1)
        with self.assertRaises(NotFound):
            await self.client.replay_parked_events(group_name, stream_name1)
        subscription_infos = await self.client.list_subscriptions_to_stream(
            str(uuid4())
        )
        self.assertEqual(subscription_infos, [])

    async def test_persistent_subscription_raises_node_is_not_leader(self) -> None:
        await self.setup_reader()
        await self.setup_writer()

        group_name = str(uuid4())
        stream_name1 = str(uuid4())
        with self.assertRaises(NodeIsNotLeader):
            await self.reader.get_subscription_info(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.list_subscriptions()

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.list_subscriptions_to_stream(stream_name1)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.create_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.create_subscription_to_all(group_name)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.update_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.read_subscription_to_all(group_name)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.read_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.update_subscription_to_all(group_name)

        # Todo: This doesn't hang...
        await self.writer.create_subscription_to_all(group_name)
        await self.writer.replay_parked_events(group_name)
        # Todo: ...but this just hangs?
        # with self.assertRaises(NodeIsNotLeader):
        #     await self.reader.replay_parked_events(group_name)

        with self.assertRaises(NodeIsNotLeader):
            await self.reader.delete_subscription(group_name)

    async def test_persistent_subscription_raises_deadline_exceeded(self) -> None:
        group_name = str(uuid4())
        stream_name1 = str(uuid4())

        await self.client.create_subscription_to_all(group_name)
        await self.client.create_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.get_subscription_info(group_name, stream_name1, timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.list_subscriptions(timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.list_subscriptions_to_stream(stream_name1, timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.create_subscription_to_stream(
                group_name, stream_name1, timeout=0
            )

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.create_subscription_to_all(group_name, timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.update_subscription_to_stream(
                group_name, stream_name1, timeout=0
            )

        # Todo: This hangs....
        # with self.assertRaises(GrpcDeadlineExceeded):
        #     await self.client.read_subscription_to_all(group_name, timeout=0)
        #
        # Todo: This hangs....
        # with self.assertRaises(GrpcDeadlineExceeded):
        #     await self.client.read_subscription_to_stream(group_name, stream_name1, timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.update_subscription_to_all(group_name, timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.replay_parked_events(group_name, timeout=0)

        with self.assertRaises(GrpcDeadlineExceeded):
            await self.client.delete_subscription(group_name, timeout=0)

    async def test_persistent_subscription_reconnects_closed_connection(self) -> None:
        group_name = str(uuid4())
        stream_name1 = str(uuid4())
        await self.client._connection.close()
        await self.client.create_subscription_to_all(group_name)

        await self.client._connection.close()
        await self.client.create_subscription_to_stream(group_name, stream_name1)

        await self.client._connection.close()
        await self.client.get_subscription_info(group_name, stream_name1)

        await self.client._connection.close()
        await self.client.list_subscriptions()

        await self.client._connection.close()
        await self.client.list_subscriptions_to_stream(stream_name1)

        await self.client._connection.close()
        await self.client.update_subscription_to_all(group_name)

        await self.client._connection.close()
        await self.client.update_subscription_to_stream(group_name, stream_name1)

        await self.client._connection.close()
        await self.client.replay_parked_events(group_name)

        await self.client._connection.close()
        s = await self.client.read_subscription_to_all(group_name)
        await s.stop()

        await self.client._connection.close()
        s = await self.client.read_subscription_to_stream(group_name, stream_name1)
        await s.stop()

        await self.client._connection.close()
        await self.client.delete_subscription(group_name)

        await self.client._connection.close()
        await self.client.delete_subscription(group_name, stream_name1)

    async def test_persistent_subscription_stop_called_twice(self) -> None:
        group_name = str(uuid4())
        await self.client._connection.close()
        await self.client.create_subscription_to_all(group_name)
        s = await self.client.read_subscription_to_all(group_name)
        await s.stop()
        self.assertTrue(cast(AsyncPersistentSubscription, s)._is_stopped)
        await s.stop()
        self.assertTrue(cast(AsyncPersistentSubscription, s)._is_stopped)

    async def test_persistent_subscription_raises_programming_error(self) -> None:
        group_name = str(uuid4())
        await self.client._connection.close()
        await self.client.create_subscription_to_all(group_name)
        s = await self.client.read_subscription_to_all(group_name)
        await s.stop()
        with self.assertRaises(ProgrammingError):
            await s.ack(uuid4())
        with self.assertRaises(ProgrammingError):
            await s.nack(uuid4(), "retry")

    async def test_persistent_subscription_read_reqs(self) -> None:
        reqs = AsyncSubscriptionReadReqs("group1", max_ack_batch_size=3)
        await reqs.__anext__()  # options req
        await reqs.ack(uuid4())
        req1 = await reqs.__anext__()  # send after queue timeout
        self.assertEqual(len(req1.ack.ids), 1)
        await reqs.ack(uuid4())
        await reqs.ack(uuid4())
        await reqs.ack(uuid4())
        req2 = await reqs.__anext__()  # send when batch full
        self.assertEqual(len(req2.ack.ids), 3)
        await reqs.ack(uuid4())
        await reqs.nack(uuid4(), "retry")
        req3 = await reqs.__anext__()  # send non-full batch because action has changed
        self.assertEqual(len(req3.ack.ids), 1)
        req4 = await reqs.__anext__()
        self.assertEqual(len(req4.nack.ids), 1)
        await reqs.ack(uuid4())
        await reqs.ack(uuid4())
        reqs._is_stopped.set()
        await reqs.stop()
        req5 = await reqs.__anext__()
        self.assertEqual(len(req5.ack.ids), 2)

        # Cover the case of stopping without waiting (wait_until_stopped=False).
        reqs = AsyncSubscriptionReadReqs("group1", max_ack_batch_size=3)
        await reqs.stop(wait_until_stopped=False)

        # Cover the case of calling this method twice.
        await reqs.stop(wait_until_stopped=False)

        # Iterate until stopped.
        async for _ in reqs:
            pass

        # Iterate and stop whilst waiting for queue item.
        reqs = AsyncSubscriptionReadReqs("group1", max_ack_batch_size=3)

        async def iterate_until_stopped() -> None:
            async for _ in reqs:
                pass

        async def sleep_then_stop() -> None:
            await asyncio.sleep(1)
            await reqs.stop(wait_until_stopped=False)

        await asyncio.gather(iterate_until_stopped(), sleep_then_stop())

        # Can't call ack() after stopped.
        with self.assertRaises(ProgrammingError):
            await reqs.ack(uuid4())
        with self.assertRaises(ProgrammingError):
            await reqs.nack(uuid4(), "park")

        # Raises exception whilst preparing batch.
        reqs = AsyncSubscriptionReadReqs("group1", max_ack_batch_size=1)
        await reqs.ack(333)  # type: ignore
        await reqs.__anext__()  # options req
        with self.assertRaises(ValueError):
            await reqs.__anext__()

    async def test_persistent_subscription_context_manager(self) -> None:
        group_name = str(uuid4())
        await self.client._connection.close()
        await self.client.create_subscription_to_all(group_name)
        s = await self.client.read_subscription_to_all(group_name)
        async with s as s:
            pass
        self.assertTrue(cast(AsyncPersistentSubscription, s)._is_stopped)

    # async def test_subscribe_to_all_raises_discovery_failed(self) -> None:
    #     await self.client._connection.close()
    #     # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
    #     await self.client._connection.close()
    #     self.client._connection = self.client._construct_esdb_connection("localhost:2222")
    #
    #     await self.client.subscribe_to_all()
    #     # with self.assertRaises(ServiceUnavailable):

    async def test_create_projection(self) -> None:
        # Create "continuous" projection.
        projection_name = str(uuid4())
        await self.client.create_projection(query="", name=projection_name)

        # Create "continuous" projection (emit enabled).
        projection_name = str(uuid4())
        await self.client.create_projection(
            query="",
            name=projection_name,
            emit_enabled=True,
        )

        # Create "continuous" projection (track emitted streams).
        projection_name = str(uuid4())
        await self.client.create_projection(
            query="",
            name=projection_name,
            emit_enabled=True,
            track_emitted_streams=True,
        )

        # Raises error if projection already exists.
        with self.assertRaises(AlreadyExists):
            await self.client.create_projection(
                query="",
                name=projection_name,
                emit_enabled=True,
                track_emitted_streams=True,
            )

        # Raises error if track_emitted=True but emit_enabled=False...
        with self.assertRaises(ExceptionThrownByHandler):
            await self.client.create_projection(
                query="",
                name=projection_name,
                emit_enabled=False,
                track_emitted_streams=True,
            )

    async def test_update_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.update_projection(name=projection_name, query="")

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Update projection.
        await self.client.update_projection(name=projection_name, query="")
        await self.client.update_projection(
            name=projection_name, query="", emit_enabled=True
        )
        await self.client.update_projection(
            name=projection_name, query="", emit_enabled=False
        )

    async def test_delete_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.delete_projection(projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Delete projection.
        await self.client.delete_projection(
            name=projection_name,
            delete_emitted_streams=True,
            delete_state_stream=True,
            delete_checkpoint_stream=True,
        )

        await asyncio.sleep(1)  # give server time to actually delete the projection....

        if "21.10" in EVENTSTORE_DOCKER_IMAGE or "22.10" in EVENTSTORE_DOCKER_IMAGE:
            # Can delete a projection that has been deleted ("idempotent").
            await self.client.delete_projection(
                name=projection_name,
            )
        else:
            # Can't delete a projection that has been deleted.
            with self.assertRaises(NotFound):
                await self.client.delete_projection(
                    name=projection_name,
                )

    async def test_get_projection_statistics(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.get_projection_statistics(name=projection_name)

        # Create named projection.
        await self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual(projection_name, statistics.name)

    async def test_disable_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.disable_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Disable projection.
        await self.client.disable_projection(name=projection_name)

    async def test_enable_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.enable_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Disable projection.
        await self.client.enable_projection(name=projection_name)

    async def test_reset_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.reset_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Reset projection.
        await self.client.reset_projection(name=projection_name)

    async def test_get_projection_state(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFound):
            await self.client.get_projection_state(name=projection_name)

        # Create named projection (query is an empty string).
        await self.client.create_projection(query="", name=projection_name)

        # Try to get projection state.
        # Todo: Why does this just hang?
        with self.assertRaises(DeadlineExceeded):
            await self.client.get_projection_state(name=projection_name, timeout=1)

        # Create named projection.
        projection_name = str(uuid4())
        await self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        # Get projection state.
        state = await self.client.get_projection_state(name=projection_name, timeout=1)
        self.assertEqual(state.value, {})

    # async def test_get_projection_result(self) -> None:
    #     projection_name = str(uuid4())
    #
    #     # Raises NotFound unless projection exists.
    #     with self.assertRaises(NotFound):
    #         await self.client.get_projection_result(name=projection_name)
    #
    #     # Create named projection.
    #     await self.client.create_projection(query="", name=projection_name)
    #
    #     # Try to get projection result.
    #     # Todo: Why does this just hang?
    #     with self.assertRaises(DeadlineExceeded):
    #         await self.client.get_projection_result(name=projection_name, timeout=1)
    #
    #     # Create named projection.
    #     projection_name = str(uuid4())
    #     await self.client.create_projection(
    #         query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
    #         name=projection_name,
    #     )
    #
    #     # Get projection result.
    #     state = await self.client.get_projection_result(name=projection_name)
    #     self.assertEqual(state.value, {})

    async def test_restart_projections_subsystem(self) -> None:
        await self.client.restart_projections_subsystem()

    async def test_projection_example(self) -> None:
        application_stream_name = "account-" + str(uuid4())
        emitted_stream_name = "emitted-" + str(uuid4())
        projection_query = (
            """
        fromStream('"""
            + application_stream_name
            + """')
        .when({
          $init: function(){
            return {
              count: 0
            };
          },
          SomethingHappened: function(s,e){
            s.count += 1;
            emit('"""
            + emitted_stream_name
            + """', 'Emitted', {}, {});
          }
        })
        .outputState()
        """
        )

        projection_name = "projection-" + str(uuid4())

        await self.client.create_projection(
            query=projection_query,
            name=projection_name,
            emit_enabled=True,
            track_emitted_streams=True,
        )
        await self.client.disable_projection(name=projection_name)

        # Set emit_enabled=False - still tracking emitted streams...
        await self.client.update_projection(
            query=projection_query,
            name=projection_name,
            emit_enabled=False,
        )

        # Set emit_enabled=True again - still tracking emitted streams...
        await self.client.update_projection(
            query=projection_query,
            name=projection_name,
            emit_enabled=True,
        )

        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual(projection_name, statistics.name)

        # Start running...
        await self.client.enable_projection(name=projection_name)

        application_events = [
            NewEvent(type="SomethingHappened", data=b"{}"),
            NewEvent(type="SomethingElseHappened", data=b"{}"),
            NewEvent(type="SomethingHappened", data=b"{}"),
        ]
        await self.client.append_events(
            stream_name=application_stream_name,
            events=application_events,
            current_version=StreamState.ANY,
        )

        # Wait for two events to have been processed.
        for _ in range(100):
            if statistics.events_processed_after_restart < 2:
                await asyncio.sleep(0.1)
                statistics = await self.client.get_projection_statistics(
                    name=projection_name
                )
                continue
            break
        else:
            self.fail("Timed out waiting for two events to be processed by projection")

        # Check projection state.
        state = await self.client.get_projection_state(name=projection_name)
        self.assertEqual(2, state.value["count"])

        # Check projection result.
        # Todo: What's the actual difference between "state" and "result"?
        #  Ans: nothing, at the moment.
        # result = await self.client.get_projection_result(name=projection_name)
        # self.assertEqual(2, result.value["count"])

        # Check project result stream.
        result_stream_name = f"$projections-{projection_name}-result"
        result_events = await self.client.get_stream(result_stream_name)
        self.assertEqual(2, len(result_events))
        self.assertEqual("Result", result_events[0].type)
        self.assertEqual("Result", result_events[1].type)

        self.assertEqual({"count": 1}, json.loads(result_events[0].data))
        self.assertEqual({"count": 2}, json.loads(result_events[1].data))

        self.assertEqual(
            str(application_events[0].id),
            json.loads(result_events[0].metadata)["$causedBy"],
        )
        self.assertEqual(
            str(application_events[2].id),
            json.loads(result_events[1].metadata)["$causedBy"],
        )

        # Check emitted event stream.
        emitted_events = await self.client.get_stream(emitted_stream_name)
        self.assertEqual(2, len(emitted_events))

        # Check projection statistics.
        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Running", statistics.status)

        # Reset whilst running is ineffective (state exists).
        await self.client.reset_projection(name=projection_name)
        await asyncio.sleep(1)
        state = await self.client.get_projection_state(name=projection_name)
        self.assertIn("count", state.value)
        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Running", statistics.status)
        self.assertLess(0, statistics.events_processed_after_restart)

        # Can't delete whilst running.
        with self.assertRaises(OperationFailed):
            await self.client.delete_projection(
                projection_name,
                delete_emitted_streams=True,
                delete_state_stream=True,
                delete_checkpoint_stream=True,
            )

        # Disable projection (stop running).
        await self.client.disable_projection(projection_name)
        await asyncio.sleep(1)
        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Stopped", statistics.status)

        # Check projection still has state.
        state = await self.client.get_projection_state(projection_name)
        self.assertIn("count", state.value)

        # Reset whilst stopped is effective (loses state)?
        await self.client.reset_projection(name=projection_name)
        await asyncio.sleep(1)
        state = await self.client.get_projection_state(projection_name)
        self.assertNotIn("count", state.value)
        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Stopped", statistics.status)
        self.assertEqual(0, statistics.events_processed_after_restart)

        # Can enable after reset.
        await self.client.enable_projection(name=projection_name)
        await asyncio.sleep(1)
        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Running", statistics.status)
        state = await self.client.get_projection_state(projection_name)
        self.assertIn("count", state.value)
        self.assertEqual(2, state.value["count"])

        # Can delete when stopped.
        await self.client.disable_projection(name=projection_name)
        await self.client.delete_projection(
            projection_name,
            delete_emitted_streams=True,
            delete_state_stream=True,
            delete_checkpoint_stream=True,
        )

        # Flaky: try/except because the projection might have been deleted already...
        try:
            statistics = await self.client.get_projection_statistics(
                name=projection_name
            )
            self.assertEqual("Deleting/Stopped", statistics.status)
        except NotFound:
            pass

        await asyncio.sleep(1)

        # After deleting, projection methods raise NotFound.
        with self.assertRaises(NotFound):
            await self.client.get_projection_statistics(name=projection_name)

        with self.assertRaises(NotFound):
            await self.client.get_projection_state(projection_name)

        # with self.assertRaises(NotFound):
        #     await self.client.get_projection_result(projection_name)

        with self.assertRaises(NotFound):
            await self.client.enable_projection(projection_name)

        with self.assertRaises(NotFound):
            await self.client.disable_projection(projection_name)

        # Result stream still exists.
        result_events = await self.client.get_stream(result_stream_name)
        self.assertEqual(2, len(result_events))

        # Emitted stream does not exist.
        with self.assertRaises(NotFound):
            await self.client.get_stream(emitted_stream_name)

        # Todo: Are "checkpoint" and "state" streams somehow hidden?

        # Todo: Recreate with same name (plus what happens if streams not deleted)...
        # self.client.create_projection(name=projection_name, query=projection_query)


class TestOptionalClientAuth(TimedTestCase, IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.user_key = b"some-key"
        self.user_cert = b"some-cert"
        self.tls_ca = b"some-cert"
        with NamedTemporaryFile(delete=False) as f1, NamedTemporaryFile(
            delete=False
        ) as f2, NamedTemporaryFile(delete=False) as f3:
            f1.write(self.user_key)
            f2.write(self.user_cert)
            f3.write(self.tls_ca)
            self.user_key_file = f1.name
            self.user_cert_file = f2.name
            self.tls_ca_file = f3.name

    def tearDown(self) -> None:
        os.remove(self.user_key_file)
        os.remove(self.user_cert_file)
        os.remove(self.tls_ca_file)

    async def test_tls_true_client_auth(self) -> None:
        secure_grpc_target = "localhost:2114"
        root_certificates = get_server_certificate(secure_grpc_target)
        uri = f"esdb://admin:changeit@{secure_grpc_target}"

        # Construct client without client auth.
        client = await AsyncioEventStoreDBClient(
            uri, root_certificates=root_certificates
        )

        # User key and cert should be None.
        self.assertIsNone(client.private_key)
        self.assertIsNone(client.certificate_chain)

        # Should be able to get commit position.
        await client.get_commit_position()

        # Construct client with client auth.
        uri += f"?UserKeyFile={self.user_key_file}&UserCertFile={self.user_cert_file}"
        client = await AsyncioEventStoreDBClient(
            uri, root_certificates=root_certificates
        )

        # User cert and key should have expected values.
        self.assertEqual(self.user_key, client.private_key)
        self.assertEqual(self.user_cert, client.certificate_chain)

        # Should raise SSL error.
        with self.assertRaises(SSLError):
            await client.get_commit_position()

        # Construct client with TlsCaFile (instead of passing root_certificates directly).
        uri += f"&TlsCaFile={self.tls_ca_file}"
        client_with_tls_ca = await AsyncioEventStoreDBClient(uri)

        # Read the contents of TlsCaFile as bytes, since root_certificates are compared as bytes
        with open(self.tls_ca_file, "rb") as f:
            tls_ca_file_contents = f.read()

        # TlsCaFile should override the root_certificates passed directly.
        self.assertNotEqual(root_certificates, client_with_tls_ca.root_certificates)
        self.assertEqual(tls_ca_file_contents, client_with_tls_ca.root_certificates)


class TestAsyncioEventStoreDBClient(TimedTestCase, IsolatedAsyncioTestCase):
    async def test_deprecated_function(self) -> None:
        self.client = await AsyncioEventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2114",
            root_certificates=get_server_certificate("localhost:2114"),
        )
        assert isinstance(self.client, _AsyncioEventStoreDBClient)
        await self.client.get_commit_position()
