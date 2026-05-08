from __future__ import annotations

import asyncio
import datetime
import json
import os
from collections import Counter
from tempfile import NamedTemporaryFile
from typing import cast
from unittest import IsolatedAsyncioTestCase, skip, skipIf
from uuid import UUID, uuid4

from kurrentdbclient import (
    AsyncPersistentSubscription,
    Checkpoint,
    NewEvent,
    StreamState,
)
from kurrentdbclient.asyncio_client import AsyncKurrentDBClient
from kurrentdbclient.common import (
    DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
    DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
    AbstractAsyncCatchupSubscription,
    AbstractAsyncPersistentSubscription,
)
from kurrentdbclient.events import CaughtUp, NewEvents
from kurrentdbclient.exceptions import (
    AlreadyExistsError,
    DeadlineExceededError,
    DiscoveryFailedError,
    ExceptionThrownByHandlerError,
    FollowerNotFoundError,
    GrpcDeadlineExceededError,
    MultiAppendToSameStreamError,
    NodeIsNotLeaderError,
    NotFoundError,
    OperationFailedError,
    ProgrammingError,
    ReadOnlyReplicaNotFoundError,
    ServiceUnavailableError,
    SSLError,
    StreamIsDeletedError,
    StreamTombstonedError,
    SubscriptionConfirmationError,
    UnauthenticatedError,
    WrongCurrentVersionError,
)
from kurrentdbclient.persistent import AsyncSubscriptionReadReqs
from kurrentdbclient.projections import ProjectionStatistics
from kurrentdbclient.streams import AsyncCatchupSubscription
from tests.test_client import (
    KURRENTDB_DOCKER_IMAGE,
    PROJECTION_QUERY_TEMPLATE1,
    SERVER_VERSION,
    TimedTestCase,
    get_ca_certificate,
    get_server_certificate,
    random_data,
)


class TestAsyncKurrentDBClient(TimedTestCase, IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = AsyncKurrentDBClient(
            uri="kdb://admin:changeit@localhost:2114",
            root_certificates=get_server_certificate("localhost:2114"),
        )
        self._reader: AsyncKurrentDBClient | None = None
        self._writer: AsyncKurrentDBClient | None = None

    @property
    def reader(self) -> AsyncKurrentDBClient:
        assert self._reader is not None
        return self._reader

    @property
    def writer(self) -> AsyncKurrentDBClient:
        assert self._writer is not None
        return self._writer

    async def setup_reader(self) -> None:
        self._reader = AsyncKurrentDBClient(
            uri="kdb://admin:changeit@localhost:2110,localhost:2110?NodePreference=follower",
            root_certificates=get_ca_certificate(),
        )

    async def setup_writer(self) -> None:
        self._writer = AsyncKurrentDBClient(
            uri="kdb://admin:changeit@localhost:2110,localhost:2110?NodePreference=leader",
            root_certificates=get_ca_certificate(),
        )

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
        except (ServiceUnavailableError, DiscoveryFailedError):
            pass
        finally:
            await super().asyncTearDown()

    async def test_connect_thread_safety(self) -> None:
        # Aquire the connection lock...
        await self.client._connection_lock.acquire()

        # Try to connect, waits for lock...
        async def connect_and_lock() -> None:
            await self.client.connect()

        async def sleep_and_connect() -> int:
            # Wait for the other to wait for the lock.
            await asyncio.sleep(0.1)
            # Actually connect.
            connection = await self.client._connect()
            self.client._connection = connection
            # Now release the lock, the other attempt won't call _connect().
            self.client._connection_lock.release()
            # Return the id of the connection.
            return id(connection)

        task1 = asyncio.create_task(connect_and_lock())
        task2 = asyncio.create_task(sleep_and_connect())

        results = await asyncio.gather(task1, task2)

        # Check the connection is unchanged.
        self.assertEqual(results[1], id(self.client._connection))

    async def test_connection_never_established_error(self) -> None:
        with self.assertRaises(ProgrammingError) as cm:
            self.client.connection  # noqa: B018
        self.assertEqual(str(cm.exception), "Connection was never established")

    async def test_esdb_scheme_discovery_single_node_cluster(self) -> None:
        client = AsyncKurrentDBClient(
            "kdb://localhost:2113,localhost:2113?Tls=False"
            "&GossipTimeout=1&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )
        await client.connect()
        self.assertEqual("localhost:2113", client.connection_target)

    async def test_esdb_discover_scheme_raises_discovery_failed(self) -> None:
        client = AsyncKurrentDBClient(
            "kdb+discover://example.com?Tls=False"
            "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )
        with self.assertRaises(DiscoveryFailedError) as cm:
            await client.connect()
        self.assertIn(":2113", str(cm.exception))
        self.assertNotIn(":9898", str(cm.exception))

        client = AsyncKurrentDBClient(
            "kdb+discover://example.com:9898?Tls=False"
            "&GossipTimeout=0&MaxDiscoverAttempts=1&DiscoveryInterval=0"
        )
        with self.assertRaises(DiscoveryFailedError) as cm:
            await client.connect()
        self.assertNotIn(":2113", str(cm.exception))
        self.assertIn(":9898", str(cm.exception))

    async def test_sometimes_reconnnects_to_selected_node_after_discovery(self) -> None:
        root_certificates = get_ca_certificate()
        client = AsyncKurrentDBClient(
            "kdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2110?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await client.connect()
        client = AsyncKurrentDBClient(
            "kdb://admin:changeit@127.0.0.1:2111,127.0.0.1:2111?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await client.connect()
        client = AsyncKurrentDBClient(
            "kdb://admin:changeit@127.0.0.1:2112,127.0.0.1:2112?NodePreference=leader",
            root_certificates=root_certificates,
        )
        await client.connect()

    async def test_node_preference_random(self) -> None:
        client = AsyncKurrentDBClient(
            "kdb://localhost:2113,localhost:2113?Tls=False&NodePreference=random"
        )
        await client.connect()

    async def test_raises_follower_not_found(self) -> None:
        client = AsyncKurrentDBClient(
            "kdb://localhost:2113,localhost:2113?Tls=False&NodePreference=follower"
        )
        with self.assertRaises(FollowerNotFoundError):
            await client.connect()

    async def test_raises_read_only_replica_not_found(self) -> None:
        client = AsyncKurrentDBClient(
            "kdb://localhost:2113,localhost:2113?Tls=False&NodePreference=readonlyreplica"
        )
        with self.assertRaises(ReadOnlyReplicaNotFoundError):
            await client.connect()

    async def test_raises_ssl_error_with_tls_true_but_no_root_certificates(
        self,
    ) -> None:
        # NB Client can work with Tls=True without setting 'root_certificates'
        # if grpc lib can verify server cert using locally installed CA certs.
        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        uri = f"kdb://admin:changeit@localhost:2114?{qs}"
        client = AsyncKurrentDBClient(uri)
        await client.connect()
        with self.assertRaises(SSLError):
            await client.get_commit_position()

    async def test_raises_ssl_error_with_tls_true_broken_root_certificates(
        self,
    ) -> None:
        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        uri = f"kdb://admin:changeit@localhost:2114?{qs}"
        client = AsyncKurrentDBClient(uri, root_certificates="blah")
        await client.connect()
        with self.assertRaises(SSLError):
            await client.get_commit_position()

    async def test_raises_discovery_failed_with_tls_true_but_no_root_certificate(
        self,
    ) -> None:
        uri = "kdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111"
        uri += "?MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

        client = AsyncKurrentDBClient(uri, root_certificates="blah")
        with self.assertRaises(DiscoveryFailedError):
            await client.connect()

    async def test_username_and_password_required_for_secure_connection(self) -> None:
        with self.assertRaises(ValueError) as cm:
            AsyncKurrentDBClient("kdb://localhost:2114")
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
        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(WrongCurrentVersionError):
            await self.client.set_stream_metadata(
                stream_name=stream_name,
                metadata=metadata,
                current_version=10,
            )

        await self.client.tombstone_stream(stream_name, current_version=StreamState.ANY)

        # Can't get metadata after tombstoning stream, because stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
            await self.client.get_stream_metadata(stream_name)

        # For some reason, we can set stream metadata, even though the stream
        # has been tombstoned, and even though we can't get stream metadata.
        # Todo: Ask DB team why this is?
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

        with self.assertRaises(StreamIsDeletedError):
            await self.client.get_stream_metadata(stream_name)

    async def test_append_events_raises_wrong_current_version(self) -> None:
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(WrongCurrentVersionError):
            await self.client.append_events(
                stream_name=stream_name1, events=[event1], current_version=10
            )

        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        event2 = NewEvent(type="OrderUpdated", data=b"{}")
        with self.assertRaises(WrongCurrentVersionError):
            await self.client.append_events(
                stream_name=stream_name1, events=[event2], current_version=10
            )

    async def test_append_events_reconnects_closed_connection(self) -> None:
        await self.client.connect()
        await self.client.connection.close()
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

    async def test_append_events_raises_service_unavailable(self) -> None:
        await self.client.connect()
        await self.client.connection.close()
        self.client.connection_spec._targets = ["localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(ServiceUnavailableError):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_append_events_raises_discovery_failed(self) -> None:
        await self.client.connect()
        await self.client.connection.close()
        self.client.connection_spec._targets = ["localhost:2222", "localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(DiscoveryFailedError):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_append_events_raises_node_is_not_leader(self) -> None:
        await self.setup_reader()
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(NodeIsNotLeaderError):
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
        with self.assertRaises(StreamIsDeletedError):
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

        with self.assertRaises(StreamIsDeletedError):
            await self.client.get_stream(stream_name=stream_name1)

    async def test_append_events_reconnects_to_leader(self) -> None:
        await self.setup_reader()
        self.reader.connection_spec.options._node_preference = "leader"

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.reader.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

    @skip("Flaky test since upgrading grpcio past v1.62")
    async def test_append_events_raises_deadline_exceeded(self) -> None:
        await self.setup_reader()
        self.reader.connection_spec.options._node_preference = "leader"

        stream_name1 = str(uuid4())
        events = [NewEvent(type="SomethingHappened", data=b"{}") for _ in range(1000)]
        with self.assertRaises(GrpcDeadlineExceededError):
            await self.reader.append_events(
                stream_name=stream_name1,
                events=events,
                current_version=StreamState.NO_STREAM,
                timeout=0,
            )

    async def test_get_stream_raises_not_found(self) -> None:
        with self.assertRaises(NotFoundError):
            await self.client.get_stream(str(uuid4()))

    async def test_get_stream_reconnects(self) -> None:
        with self.assertRaises(NotFoundError):
            await self.client.get_stream(str(uuid4()))
        await self.client.connection.close()
        with self.assertRaises(NotFoundError):
            await self.client.get_stream(str(uuid4()))

    async def test_get_stream_raises_service_unavailable(self) -> None:
        self.client.connection_spec._targets = ["localhost:2222"]
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(ServiceUnavailableError):
            await self.client.append_events(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            )

    async def test_delete_stream_raises_stream_not_found(self) -> None:
        stream_name1 = str(uuid4())

        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(WrongCurrentVersionError):
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

        with self.assertRaises(StreamIsDeletedError):
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
        self.reader.connection_spec.options._node_preference = "leader"

        await self.reader.delete_stream(stream_name1, current_version=0)

    async def test_tombstone_stream_raises_stream_not_found(self) -> None:
        stream_name1 = str(uuid4())

        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(WrongCurrentVersionError):
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

        with self.assertRaises(StreamIsDeletedError):
            await self.client.tombstone_stream(stream_name1, current_version=0)

    async def test_tombstone_stream_reconnects_to_leader(self) -> None:
        await self.setup_writer()
        await self.writer.connect()

        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        await self.writer.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        await self.setup_reader()
        await self.reader.connect()
        self.reader.connection_spec.options._node_preference = "leader"

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

    async def test_subscribe_to_all_can_be_stopped(self) -> None:
        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe from the beginning.
        subscription = await self.client.subscribe_to_all()

        # Stop subscription.
        await subscription.stop()

        # Iterating should stop.
        async for _ in subscription:
            pass

        # Exiting the context manager should stop the subscription.
        subscription = await self.client.subscribe_to_all()
        async with subscription:
            pass
        self.assertTrue(cast(AsyncCatchupSubscription, subscription)._is_stopped)

        # Calling stop inside the context manager should terminate the iteration.
        subscription = await self.client.subscribe_to_all()
        async with subscription:
            await subscription.stop()
            async for _ in subscription:
                pass
        self.assertTrue(cast(AsyncCatchupSubscription, subscription)._is_stopped)

    async def test_subscribe_to_stream_with_gather_all_complete(self) -> None:
        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        event2 = NewEvent(type="OrderCreated", data=b"{}")
        event3 = NewEvent(type="OrderUpdated", data=b"{}")
        event4 = NewEvent(type="OrderUpdated", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1, event2],
            current_version=StreamState.NO_STREAM,
        )

        stream_name2 = str(uuid4())
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event3, event4],
            current_version=StreamState.NO_STREAM,
        )

        class Worker:
            def __init__(
                self, client: AsyncKurrentDBClient, stream_name: str, event_id: UUID
            ) -> None:
                self.client = client
                self.stream_name = stream_name
                self.event_id = event_id

            async def run(self) -> None:
                async with await self.client.subscribe_to_stream(
                    stream_name=self.stream_name
                ) as subscription:
                    async for event in subscription:
                        if event.id == self.event_id:
                            break

        await asyncio.gather(
            Worker(self.client, stream_name1, event1.id).run(),
            Worker(self.client, stream_name2, event3.id).run(),
        )

        # Important to know calling stop() doesn't cancel the current task.
        current_task = asyncio.current_task()
        if current_task is not None:
            self.assertFalse(current_task.cancelled())

    async def test_subscribe_to_all_with_task_cancel_no_context_manager(self) -> None:

        at_async_for = asyncio.Event()

        class Worker:
            def __init__(self, subscription: AbstractAsyncCatchupSubscription) -> None:
                self.subscription = subscription
                self.was_cancelled = False

            async def run(self) -> None:
                at_async_for.set()
                try:
                    async for event in self.subscription:
                        msg = f"async for didn't raise asyncio.CancelledError {event}"
                        raise AssertionError(msg)
                except asyncio.CancelledError:
                    self.was_cancelled = True
                    raise

        subscription = await self.client.subscribe_to_stream(str(uuid4()))
        worker = Worker(subscription)
        task = asyncio.create_task(worker.run())
        await at_async_for.wait()
        await asyncio.sleep(0.1)  # Try to make sure we got into _get_next_read_resp
        # await asyncio.sleep(10)  # Try to make sure we got into _get_next_read_resp
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(worker.was_cancelled)

    async def test_subscribe_to_all_with_task_cancel_with_context_manager(self) -> None:

        at_async_for = asyncio.Event()

        class Worker:
            def __init__(self, subscription: AbstractAsyncCatchupSubscription) -> None:
                self.subscription = subscription
                self.was_cancelled = False

            async def run(self) -> None:
                at_async_for.set()
                try:
                    async for event in self.subscription:
                        msg = f"async for didn't raise asyncio.CancelledError {event}"
                        raise AssertionError(msg)
                except asyncio.CancelledError:
                    self.was_cancelled = True
                    raise

        async with await self.client.subscribe_to_stream(str(uuid4())) as subscription:
            worker = Worker(subscription)
            task = asyncio.create_task(worker.run())
            await at_async_for.wait()
            await asyncio.sleep(0.1)  # Try to make sure we got into _get_next_read_resp
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(worker.was_cancelled)

    async def test_subscribe_to_all_reconnects(self) -> None:
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
        "21.10" in KURRENTDB_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    @skipIf(
        "22.10" in KURRENTDB_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    async def test_subscribe_to_all_include_caught_up(self) -> None:
        commit_position = await self.client.get_commit_position()

        # Append new events.
        before_recording = datetime.datetime.now(tz=datetime.timezone.utc)
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
                if "23.10" in KURRENTDB_DOCKER_IMAGE:
                    pass
                else:
                    self.assertEqual(0, event.stream_position)
                    self.assertEqual(commit_position, event.commit_position)
                    self.assertEqual(commit_position, event.prepare_position)
                    assert event.recorded_at is not None
                    self.assertGreaterEqual(event.recorded_at, before_recording)
                    after_subscribing = datetime.datetime.now(tz=datetime.timezone.utc)
                    # Note, difference with server clock makes this fail occasionally.
                    self.assertLessEqual(event.recorded_at, after_subscribing)
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
        with self.assertRaises(NotFoundError):
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
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        "21.10" in KURRENTDB_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    @skipIf(
        "22.10" in KURRENTDB_DOCKER_IMAGE,
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    async def test_subscribe_to_stream_include_caught_up(self) -> None:
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append new events.
        before_recording = datetime.datetime.now(tz=datetime.timezone.utc)
        stream_name1 = str(uuid4())
        commit_position = await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )

        # Subscribe to stream events, from the start.
        subscription = await self.client.subscribe_to_stream(
            stream_name=stream_name1,
            include_caught_up=True,
            timeout=10,
        )
        async for event in subscription:
            if isinstance(event, CaughtUp):
                if "23.10" in KURRENTDB_DOCKER_IMAGE:
                    pass
                else:
                    self.assertEqual(1, event.stream_position)
                    self.assertNotEqual(commit_position, event.commit_position)
                    self.assertNotEqual(commit_position, event.prepare_position)
                    self.assertEqual(0, event.prepare_position)
                    self.assertEqual(1, event.stream_position)
                    assert event.recorded_at is not None
                    self.assertGreaterEqual(event.recorded_at, before_recording)
                    after_subscribing = datetime.datetime.now(tz=datetime.timezone.utc)
                    self.assertLessEqual(event.recorded_at, after_subscribing)
                break

    async def test_persistent_subscription_to_all(self) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(ValueError) as cm:
            async for _ in persistent_subscription:
                await persistent_subscription.ack("a")  # type: ignore[arg-type]
        self.assertIn("event_id 'a' is not a UUID", str(cm.exception))

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
        with self.assertRaises(NotFoundError):
            await self.client.read_subscription_to_all(group_name)

        subscription_infos = await self.client.list_subscriptions()
        for subscription_info in subscription_infos:
            if subscription_info.group_name == group_name:
                self.fail("Subscription found in list")

        # - raises NotFound
        with self.assertRaises(NotFoundError):
            await self.client.read_subscription_to_all(group_name)
        with self.assertRaises(NotFoundError):
            await self.client.update_subscription_to_all(group_name)
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name)
        with self.assertRaises(NotFoundError):
            await self.client.replay_parked_events(group_name)

    async def test_persistent_subscription_init_error_confirmation_is_none(
        self,
    ) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name)

        # Create subscription.
        await self.client.create_subscription_to_all(group_name, from_end=True)

        # Consume the subscription.
        persistent_subscription = cast(
            AsyncPersistentSubscription,
            await self.client.read_subscription_to_all(group_name),
        )
        with self.assertRaises(SubscriptionConfirmationError) as cm:
            async with persistent_subscription:
                await persistent_subscription._read_resp_queue.put(None)
                # Wrongly call init() again, should choke on the None.
                await persistent_subscription.init()

        self.assertIn(
            "Expected subscription confirmation, got: None", str(cm.exception)
        )

    async def test_persistent_subscription_init_error_confirmation_is_event(
        self,
    ) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name)

        # Create subscription.
        await self.client.create_subscription_to_all(group_name, from_end=True)

        # Append an event
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated1", data=b"{}")
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        # Consume the subscription.
        persistent_subscription = cast(
            AsyncPersistentSubscription,
            await self.client.read_subscription_to_all(group_name),
        )
        with self.assertRaises(SubscriptionConfirmationError) as cm:
            async with persistent_subscription:
                # Wrongly call init() again, should choke on the event.
                await persistent_subscription.init()

        self.assertIn(
            "Expected subscription confirmation, got: event", str(cm.exception)
        )

    async def test_persistent_subscription_init_error_confirmation_wrong_group(
        self,
    ) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name)

        # Create subscription.
        await self.client.create_subscription_to_all(group_name, from_end=True)

        # Consume the subscription.
        read_reqs = AsyncSubscriptionReadReqs(group_name=group_name)
        stream_stream_call = self.client.connection.persistent_subscriptions._stub.Read(
            read_reqs,
            metadata=self.client._call_metadata,
            credentials=self.client._call_credentials,
        )

        wrong_group_name = group_name + "wrong"
        persistent_subscription = AsyncPersistentSubscription(
            read_reqs=read_reqs,
            stream_stream_call=stream_stream_call,
            expected_group_name=wrong_group_name,
            stream_name=None,
            grpc_streamers=self.client.connection._grpc_streamers,
        )

        with self.assertRaises(SubscriptionConfirmationError) as cm:
            async with persistent_subscription:
                # First time calling init(), but expected group name in wrong.
                await persistent_subscription.init()

        self.assertIn(f"Expected group name: {wrong_group_name}", str(cm.exception))

    async def test_read_subscription_to_all_with_task_cancel_no_context_manager(
        self,
    ) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name)

        # Create subscription.
        await self.client.create_subscription_to_all(group_name, from_end=True)

        at_async_for = asyncio.Event()

        class Worker:
            def __init__(
                self, subscription: AbstractAsyncPersistentSubscription
            ) -> None:
                self.subscription = subscription
                self.was_cancelled = False

            async def run(self) -> None:
                at_async_for.set()
                try:
                    async for event in self.subscription:
                        msg = f"async for didn't raise asyncio.CancelledError {event}"
                        raise AssertionError(msg)
                except asyncio.CancelledError:
                    self.was_cancelled = True
                    raise

        subscription = await self.client.read_subscription_to_all(group_name)
        worker = Worker(subscription)
        task = asyncio.create_task(worker.run())
        await at_async_for.wait()
        await asyncio.sleep(0.1)  # Try to make sure we got into _get_next_read_resp
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(worker.was_cancelled)

    async def test_read_subscription_to_all_with_task_cancel_with_context_manager(
        self,
    ) -> None:
        # Check subscription does not exist.
        group_name = str(uuid4())
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name)

        # Create subscription.
        await self.client.create_subscription_to_all(group_name, from_end=True)

        at_async_for = asyncio.Event()

        class Worker:
            def __init__(
                self, subscription: AbstractAsyncPersistentSubscription
            ) -> None:
                self.subscription = subscription
                self.was_cancelled = False

            async def run(self) -> None:
                at_async_for.set()
                try:
                    async for event in self.subscription:
                        msg = f"async for didn't raise asyncio.CancelledError {event}"
                        raise AssertionError(msg)
                except asyncio.CancelledError:
                    self.was_cancelled = True
                    raise

        async with await self.client.read_subscription_to_all(
            group_name
        ) as subscription:
            worker = Worker(subscription)
            task = asyncio.create_task(worker.run())
            await at_async_for.wait()
            await asyncio.sleep(0.1)  # Try to make sure we got into _get_next_read_resp
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(worker.was_cancelled)

    async def test_subscription_to_all_update(self) -> None:
        group_name = f"my-subscription-{uuid4().hex}"

        # Can't update subscription that doesn't exist.
        with self.assertRaises(NotFoundError):
            # raises in get_info()
            await self.client.update_subscription_to_all(group_name=group_name)
        with self.assertRaises(NotFoundError):
            # raises in update()
            await self.client.connection.persistent_subscriptions.update(
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
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        )
        self.assertEqual(info.extra_statistics, False)

        await self.client.update_subscription_to_all(
            group_name=group_name, consumer_strategy="Pinned"
        )

        info = await self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")
        self.assertEqual(info.resolve_links, True)
        self.assertEqual(info.consumer_strategy, "Pinned")
        self.assertEqual(info.message_timeout, DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT)
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.max_retry_count, DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT)
        self.assertEqual(
            info.min_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        )
        self.assertEqual(
            info.max_checkpoint_count,
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        )
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.checkpoint_after, DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER)
        self.assertEqual(
            info.max_subscriber_count,
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        )
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.live_buffer_size, DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE)
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        self.assertEqual(info.read_batch_size, DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE)
        self.assertEqual(
            info.history_buffer_size,
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
            DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
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
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(ValueError) as cm:
            async for _ in subscription:
                await subscription.ack("a")  # type: ignore[arg-type]
        self.assertIn("event_id 'a' is not a UUID", str(cm.exception))

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
        with self.assertRaises(NotFoundError):
            await self.client.read_subscription_to_stream(group_name, stream_name1)
        with self.assertRaises(NotFoundError):
            await self.client.update_subscription_to_stream(group_name, stream_name1)
        with self.assertRaises(NotFoundError):
            await self.client.get_subscription_info(group_name, stream_name1)
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.get_subscription_info(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.list_subscriptions()

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.list_subscriptions_to_stream(stream_name1)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.create_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.create_subscription_to_all(group_name)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.update_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.read_subscription_to_all(group_name)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.read_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.update_subscription_to_all(group_name)

        # Todo: This doesn't hang...
        await self.writer.create_subscription_to_all(group_name)
        await self.writer.replay_parked_events(group_name)
        # Todo: ...but this just hangs?
        # with self.assertRaises(NodeIsNotLeader):
        #     await self.reader.replay_parked_events(group_name)

        with self.assertRaises(NodeIsNotLeaderError):
            await self.reader.delete_subscription(group_name)

    @skip("Flaky test since upgrading grpcio past v1.62")
    async def test_persistent_subscription_raises_deadline_exceeded(self) -> None:
        group_name = str(uuid4())
        stream_name1 = str(uuid4())

        await self.client.create_subscription_to_all(group_name)
        await self.client.create_subscription_to_stream(group_name, stream_name1)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.get_subscription_info(group_name, stream_name1, timeout=0)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.list_subscriptions(timeout=0)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.list_subscriptions_to_stream(stream_name1, timeout=0)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.create_subscription_to_stream(
                group_name, stream_name1, timeout=0
            )

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.create_subscription_to_all(group_name, timeout=0)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.update_subscription_to_stream(
                group_name, stream_name1, timeout=0
            )

        # Todo: This hangs....
        # with self.assertRaises(GrpcDeadlineExceeded):
        #     await self.client.read_subscription_to_all(group_name, timeout=0)
        #
        # Todo: This hangs....
        # with self.assertRaises(GrpcDeadlineExceeded):
        #     await self.client.read_subscription_to_stream(
        #         group_name, stream_name1, timeout=0
        #     )

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.update_subscription_to_all(group_name, timeout=0)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.replay_parked_events(group_name, timeout=0)

        with self.assertRaises(GrpcDeadlineExceededError):
            await self.client.delete_subscription(group_name, timeout=0)

    async def test_persistent_subscription_reconnects_closed_connection(self) -> None:
        group_name = str(uuid4())
        stream_name1 = str(uuid4())
        await self.client.connect()

        await self.client.connection.close()
        await self.client.create_subscription_to_all(group_name)

        await self.client.connection.close()
        await self.client.create_subscription_to_stream(group_name, stream_name1)

        await self.client.connection.close()
        await self.client.get_subscription_info(group_name, stream_name1)

        await self.client.connection.close()
        await self.client.list_subscriptions()

        await self.client.connection.close()
        await self.client.list_subscriptions_to_stream(stream_name1)

        await self.client.connection.close()
        await self.client.update_subscription_to_all(group_name)

        await self.client.connection.close()
        await self.client.update_subscription_to_stream(group_name, stream_name1)

        await self.client.connection.close()
        await self.client.replay_parked_events(group_name)

        await self.client.connection.close()
        s = await self.client.read_subscription_to_all(group_name)
        await s.stop()

        await self.client.connection.close()
        s = await self.client.read_subscription_to_stream(group_name, stream_name1)
        await s.stop()

        await self.client.connection.close()
        await self.client.delete_subscription(group_name)

        await self.client.connection.close()
        await self.client.delete_subscription(group_name, stream_name1)

    async def test_persistent_subscription_stop_called_twice(self) -> None:
        group_name = str(uuid4())
        await self.client.create_subscription_to_all(group_name)
        s = await self.client.read_subscription_to_all(group_name)
        await s.stop()
        self.assertTrue(cast(AsyncPersistentSubscription, s)._is_stopped)
        await s.stop()
        self.assertTrue(cast(AsyncPersistentSubscription, s)._is_stopped)

    async def test_persistent_subscription_ack_after_stop(self) -> None:
        group_name = str(uuid4())
        await self.client.create_subscription_to_all(group_name)

        # Can't ack after subscription has been stopped (not context manager).
        s = await self.client.read_subscription_to_all(group_name)
        await s.stop()
        with self.assertRaises(ProgrammingError):
            await s.ack(uuid4())
        with self.assertRaises(ProgrammingError):
            await s.nack(uuid4(), "retry")

        # Can ack after subscription has been stopped (is context manager).
        s = await self.client.read_subscription_to_all(group_name)
        async with s:
            await s.stop()
            # with self.assertRaises(ProgrammingError):
            await s.ack(uuid4())
            # with self.assertRaises(ProgrammingError):
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
        # await self.client._connection.close()
        await self.client.create_subscription_to_all(group_name)

        # Exiting the context manager should stop the subscription.
        consumer = await self.client.read_subscription_to_all(group_name)
        async with consumer:
            pass
        self.assertTrue(cast(AsyncPersistentSubscription, consumer)._is_stopped)

        # Calling stop inside the context manager should terminate the iteration.
        async with await self.client.read_subscription_to_all(group_name) as consumer:
            await consumer.stop()
            async for _ in consumer:
                pass

    # async def test_subscribe_to_all_raises_discovery_failed(self) -> None:
    #     await self.client._connection.close()
    #     # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
    #     await self.client._connection.close()
    #     self.client._connection = self.client._construct_esdb_connection(
    #         "localhost:2222"
    #     )
    #
    #     await self.client.subscribe_to_all()
    #     # with self.assertRaises(ServiceUnavailable):

    async def test_subscription_to_all_event_not_redelivered_after_ack(self) -> None:
        # This test was added to ensure that acks are effective, which requires
        # acks are send with the received subscriber_id. This wasn't being done
        # in the async client, as reported by Bruno van de Werve in:
        # https://github.com/pyeventsourcing/kurrentdbclient/issues/35

        # Create persistent subscription.
        group_name1 = f"my-subscription-{uuid4().hex}"
        stream_name1 = str(uuid4())
        max_retry_count = 3
        await self.client.create_subscription_to_all(
            group_name=group_name1,
            from_end=True,
            message_timeout=1,
            max_retry_count=max_retry_count,
        )
        print("Created persistent subscription")

        # Start consumer.
        subscription1 = await self.client.read_subscription_to_all(
            group_name=group_name1
        )
        print("Started persistent subscription consumer #1")

        # Append some events.
        num_appended_events = 1
        events = [
            NewEvent(type="SomethingHappened", data=random_data(), metadata=b"{}")
            for _ in range(num_appended_events)
        ]
        await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=events,
        )
        for new_event in events:
            print("Appended event:", new_event.id)
        appended_event_ids = {e.id for e in events}

        print("Consuming appended events...")
        acked_event_ids = set[UUID]()
        async with subscription1:
            while len(acked_event_ids) < num_appended_events:
                event = await subscription1.__anext__()
                if event.id not in appended_event_ids:
                    continue  # Ignore any other events (shouldn't get here)
                print("Received event:", event.id)
                if event.id in acked_event_ids:
                    self.fail(f"Acked event was redelivered: {event.id}")
                await subscription1.ack(event)
                print("Acked event:", event.id)
                acked_event_ids.add(event.id)

        print("Stopped persistent subscription consumer #1")

        # Append some more events.
        events = [
            NewEvent(type="SomethingHappened", data=random_data(), metadata=b"{}")
            for _ in range(num_appended_events)
        ]
        await self.client.append_events(
            stream_name1,
            current_version=num_appended_events - 1,
            events=events,
        )
        for new_event in events:
            print("Appended event:", new_event.id)

        # Start another consumer.
        subscription2 = await self.client.read_subscription_to_all(
            group_name=group_name1
        )
        print("Started persistent subscription consumer #2")
        unacked_events_received = Counter[UUID]()
        async with subscription2:
            while (
                unacked_events_received.total() < max_retry_count * num_appended_events
            ):
                event = await subscription2.__anext__()
                if event.id in acked_event_ids:
                    self.fail("Acked event was redelivered")
                print("Received event:", event.id)
                unacked_events_received.update([event.id])
        print("Stopped persistent subscription consumer #2")
        print("None of the acked events were redelivered")

    async def test_subscription_to_stream_event_not_redelivered_after_ack(
        self,
    ) -> None:
        # This test was added to ensure that acks are effective, which requires
        # acks are send with the received subscriber_id. This wasn't being done
        # in the async client, as reported by Bruno van de Werve in:
        # https://github.com/pyeventsourcing/kurrentdbclient/issues/35

        # Create persistent subscription.
        group_name1 = f"my-subscription-{uuid4().hex}"
        stream_name1 = str(uuid4())
        max_retry_count = 3
        await self.client.create_subscription_to_stream(
            group_name=group_name1,
            stream_name=stream_name1,
            from_end=True,
            message_timeout=1,
            max_retry_count=max_retry_count,
        )
        print("Created persistent subscription")

        # Start consumer.
        subscription1 = await self.client.read_subscription_to_stream(
            group_name=group_name1,
            stream_name=stream_name1,
        )
        print("Started persistent subscription consumer #1")

        # Append some events.
        num_appended_events = 1
        events = [
            NewEvent(type="SomethingHappened", data=random_data(), metadata=b"{}")
            for _ in range(num_appended_events)
        ]
        await self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=events,
        )
        for new_event in events:
            print("Appended event:", new_event.id)
        appended_event_ids = {e.id for e in events}

        print("Consuming appended events...")
        acked_event_ids = set[UUID]()
        async with subscription1:
            while len(acked_event_ids) < num_appended_events:
                event = await subscription1.__anext__()
                if event.id not in appended_event_ids:
                    continue  # Ignore any other events (shouldn't get here)
                print("Received event:", event.id)
                if event.id in acked_event_ids:
                    self.fail(f"Acked event was redelivered: {event.id}")
                await subscription1.ack(event)
                print("Acked event:", event.id)
                acked_event_ids.add(event.id)

        print("Stopped persistent subscription consumer #1")

        # Append some more events.
        events = [
            NewEvent(type="SomethingHappened", data=random_data(), metadata=b"{}")
            for _ in range(num_appended_events)
        ]
        await self.client.append_events(
            stream_name1,
            current_version=num_appended_events - 1,
            events=events,
        )
        for new_event in events:
            print("Appended event:", new_event.id)

        # Start another consumer.
        subscription2 = await self.client.read_subscription_to_stream(
            group_name=group_name1,
            stream_name=stream_name1,
        )
        print("Started persistent subscription consumer #2")
        unacked_events_received = Counter[UUID]()
        async with subscription2:
            while (
                unacked_events_received.total() < max_retry_count * num_appended_events
            ):
                event = await subscription2.__anext__()
                if event.id in acked_event_ids:
                    self.fail("Acked event was redelivered")
                print("Received event:", event.id)
                unacked_events_received.update([event.id])
        print("Stopped persistent subscription consumer #2")
        print("None of the acked events were redelivered")

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
        with self.assertRaises(AlreadyExistsError):
            await self.client.create_projection(
                query="",
                name=projection_name,
                emit_enabled=True,
                track_emitted_streams=True,
            )

        # Raises error if track_emitted=True but emit_enabled=False...
        with self.assertRaises(ExceptionThrownByHandlerError):
            await self.client.create_projection(
                query="",
                name=projection_name,
                emit_enabled=False,
                track_emitted_streams=True,
            )

    async def test_update_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(NotFoundError):
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

        if "21.10" in KURRENTDB_DOCKER_IMAGE or "22.10" in KURRENTDB_DOCKER_IMAGE:
            # Can delete a projection that has been deleted ("idempotent").
            await self.client.delete_projection(
                name=projection_name,
            )
        else:
            # Can't delete a projection that has been deleted.
            with self.assertRaises(NotFoundError):
                await self.client.delete_projection(
                    name=projection_name,
                )

    async def test_get_projection_statistics(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            await self.client.get_projection_statistics(name=projection_name)

        # Create named projection.
        await self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        statistics = await self.client.get_projection_statistics(name=projection_name)
        self.assertEqual(projection_name, statistics.name)

    async def test_list_all_projection_statistics(self) -> None:
        projection_name = str(uuid4())

        # Create named projection.
        await self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )
        await asyncio.sleep(0.5)

        statistics = await self.client.list_all_projection_statistics()
        self.assertIsInstance(statistics, list)
        self.assertGreater(len(statistics), 0)
        self.assertIsInstance(statistics[0], ProjectionStatistics)

    async def test_list_continuous_projection_statistics(self) -> None:
        projection_name = str(uuid4())

        # Create named projection.
        await self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        await asyncio.sleep(0.5)
        statistics = await self.client.list_continuous_projection_statistics()
        self.assertIsInstance(statistics, list)
        self.assertGreater(len(statistics), 0)
        self.assertIsInstance(statistics[0], ProjectionStatistics)

    async def test_disable_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            await self.client.disable_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Disable projection.
        await self.client.disable_projection(name=projection_name)

    async def test_abort_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            await self.client.disable_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Abort projection.
        await self.client.abort_projection(name=projection_name)

    async def test_enable_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            await self.client.enable_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Disable projection.
        await self.client.enable_projection(name=projection_name)

    async def test_reset_projection(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            await self.client.reset_projection(name=projection_name)

        # Create named projection.
        await self.client.create_projection(query="", name=projection_name)

        # Reset projection.
        await self.client.reset_projection(name=projection_name)

    async def test_get_projection_state(self) -> None:
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            await self.client.get_projection_state(name=projection_name)

        # Create named projection (query is an empty string).
        await self.client.create_projection(query="", name=projection_name)

        # Try to get projection state.
        # Todo: Why does this just hang?
        with self.assertRaises(DeadlineExceededError):
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

    async def test_get_projection_state_partition(self) -> None:
        stream_name = "stream-partitioned-projection-" + str(uuid4())
        projection_name = str(uuid4())
        projection_query = (
            """
            fromStream('"""
            + stream_name
            + """')
            .partitionBy(function(event) {
                return event.data.partition;
            })
            .when({
                $init: function(){
                    return {
                        count: 0
                    };
                },
                PartitionedEvent: function(state, event){
                    state.count += 1;
                    return state;
                }
            });
            """
        )

        events = [
            NewEvent(type="PartitionedEvent", data=b'{"partition": 1}'),
            NewEvent(type="PartitionedEvent", data=b'{"partition": 2}'),
            NewEvent(type="PartitionedEvent", data=b'{"partition": 3}'),
            NewEvent(type="PartitionedEvent", data=b'{"partition": 3}'),
        ]
        await self.client.append_events(
            stream_name=stream_name,
            events=events,
            current_version=StreamState.ANY,
        )

        # Create named projection (query is an empty string).
        await self.client.create_projection(
            query=projection_query, name=projection_name
        )

        statistics = await self.client.get_projection_statistics(name=projection_name)

        # Wait for four events to have been processed.
        for _ in range(100):
            if statistics.events_processed_after_restart < 3:
                await asyncio.sleep(0.1)
                statistics = await self.client.get_projection_statistics(
                    name=projection_name
                )
                continue
            break
        else:
            self.fail(
                "Timed out waiting for three events to be processed by projection"
            )

        state = await self.client.get_projection_state(
            name=projection_name, partition="1"
        )
        self.assertEqual(1, state.value["count"])
        state = await self.client.get_projection_state(
            name=projection_name, partition="2"
        )
        self.assertEqual(1, state.value["count"])
        state = await self.client.get_projection_state(
            name=projection_name, partition="3"
        )
        self.assertEqual(2, state.value["count"])

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
        with self.assertRaises(OperationFailedError):
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
        except NotFoundError:
            pass

        await asyncio.sleep(1)

        # After deleting, projection methods raise NotFound.
        with self.assertRaises(NotFoundError):
            await self.client.get_projection_statistics(name=projection_name)

        with self.assertRaises(NotFoundError):
            await self.client.get_projection_state(projection_name)

        # with self.assertRaises(NotFound):
        #     await self.client.get_projection_result(projection_name)

        with self.assertRaises(NotFoundError):
            await self.client.enable_projection(projection_name)

        with self.assertRaises(NotFoundError):
            await self.client.disable_projection(projection_name)

        # Result stream still exists.
        result_events = await self.client.get_stream(result_stream_name)
        self.assertEqual(2, len(result_events))

        # Emitted stream does not exist.
        with self.assertRaises(NotFoundError):
            await self.client.get_stream(emitted_stream_name)

        # Todo: Are "checkpoint" and "state" streams somehow hidden?

        # Todo: Recreate with same name (plus what happens if streams not deleted)...
        # self.client.create_projection(name=projection_name, query=projection_query)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_one_stream(self) -> None:
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFoundError):
            await self.client.get_stream(stream_name)

        # Check stream position is None.
        self.assertEqual(
            await self.client.get_current_version(stream_name), StreamState.NO_STREAM
        )

        # Construct four new events.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        event2 = NewEvent(
            type="OrderUpdated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        event3 = NewEvent(
            type="OrderDeleted",
            data=random_data(),
            content_type="application/octet-stream",
        )
        event4 = NewEvent(
            type="OrderCorrected",
            data=random_data(),
            content_type="application/octet-stream",
        )

        # Check get error when attempting to append new event to position 1.
        with self.assertRaises(WrongCurrentVersionError) as cm:
            await self.client.multi_append_to_stream(
                NewEvents(stream_name, current_version=1, events=[event1])
            )
        self.assertEqual(
            f"Append failed due to a version conflict on stream {stream_name!r}. "
            f"Expected version: 1. Actual version: -1.",
            cm.exception.args[0],
        )

        # Check get error when attempting to append new event expecting stream exists.
        with self.assertRaises(WrongCurrentVersionError) as cm:
            await self.client.multi_append_to_stream(
                NewEvents(
                    stream_name, current_version=StreamState.EXISTS, events=[event1]
                )
            )
        self.assertEqual(
            f"Append failed due to a version conflict on stream {stream_name!r}. "
            f"Expected version: -4. Actual version: -1.",
            cm.exception.args[0],
        )

        # Check the current_version value is validated.
        with self.assertRaises(ProgrammingError) as cm_prog_err:
            await self.client.multi_append_to_stream(
                NewEvents(stream_name, current_version=-1, events=[event1])
            )
        self.assertEqual(
            "Unsupported current_version value: -1", cm_prog_err.exception.args[0]
        )

        # Append new event with correct expected position of StreamState.NO_STREAM.
        commit_position0 = await self.client.get_commit_position()
        commit_position1 = await self.client.multi_append_to_stream(
            NewEvents(
                stream_name, current_version=StreamState.NO_STREAM, events=[event1]
            )
        )

        # Check commit position is greater.
        self.assertGreater(commit_position1, commit_position0)

        # Check stream position is 0.
        self.assertEqual(await self.client.get_current_version(stream_name), 0)

        # Read the stream forwards from the start (expect one event).
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 1)

        # Check the attributes of the recorded event.
        self.assertEqual(events[0].type, event1.type)
        self.assertEqual(events[0].data, event1.data)
        self.assertEqual(events[0].content_type, event1.content_type)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[0].stream_name, stream_name)
        self.assertEqual(events[0].stream_position, 0)
        self.assertEqual(events[0].commit_position, commit_position1)

        # Check we can't append another new event at initial position.
        with self.assertRaises(WrongCurrentVersionError) as cm:
            await self.client.multi_append_to_stream(
                NewEvents(
                    stream_name, current_version=StreamState.NO_STREAM, events=[event2]
                )
            )
        self.assertEqual(
            f"Append failed due to a version conflict on stream '{stream_name}'. "
            f"Expected version: -1. Actual version: 0.",
            cm.exception.args[0],
        )

        # Append another event.
        commit_position2 = await self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=0, events=[event2])
        )

        # Check stream position is 1.
        self.assertEqual(await self.client.get_current_version(stream_name), 1)

        # Check stream position.
        self.assertGreater(commit_position2, commit_position1)

        # Read the stream (expect two events in 'forwards' order).
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

        # Read the stream backwards from the end.
        events = await self.client.get_stream(stream_name, backwards=True)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event1.id)

        # Read the stream forwards from position 1.
        events = await self.client.get_stream(stream_name, stream_position=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Read the stream backwards from position 0.
        events = await self.client.get_stream(
            stream_name, stream_position=0, backwards=True
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Read the stream forwards from start with limit.
        events = await self.client.get_stream(stream_name, limit=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Read the stream backwards from end with limit.
        events = await self.client.get_stream(stream_name, backwards=True, limit=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Check we can't append another new event at second position.
        with self.assertRaises(WrongCurrentVersionError) as cm:
            await self.client.multi_append_to_stream(
                NewEvents(stream_name, current_version=0, events=[event3])
            )
        self.assertEqual(
            f"Append failed due to a version conflict on stream '{stream_name}'. "
            f"Expected version: 0. Actual version: 1.",
            cm.exception.args[0],
        )

        # Append another new event.
        commit_position3 = await self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=1, events=[event3])
        )

        # Check stream position is 2.
        self.assertEqual(await self.client.get_current_version(stream_name), 2)

        # Check the commit position.
        self.assertGreater(commit_position3, commit_position2)

        # Read the stream forwards from start (expect three events).
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Read the stream backwards from end (expect three events).
        events = await self.client.get_stream(stream_name, backwards=True)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event1.id)

        # Read the stream forwards from position 1 with limit 1.
        events = await self.client.get_stream(stream_name, stream_position=1, limit=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Read the stream backwards from position 1 with limit 1.
        events = await self.client.get_stream(
            stream_name, stream_position=1, backwards=True, limit=1
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Idempotent write of event1.
        commit_position1_1 = await self.client.multi_append_to_stream(
            NewEvents(
                stream_name, current_version=StreamState.NO_STREAM, events=[event1]
            )
        )
        self.assertEqual(commit_position1, commit_position1_1)

        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event2.
        commit_position2_1 = await self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=0, events=[event2])
        )
        self.assertEqual(commit_position2_1, commit_position2)

        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event3.
        commit_position3_1 = await self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=1, events=[event3])
        )
        self.assertEqual(commit_position3, commit_position3_1)

        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event1, event2.
        commit_position2_1 = await self.client.multi_append_to_stream(
            NewEvents(
                stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event1, event2],
            )
        )
        self.assertEqual(commit_position2, commit_position2_1)

        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event2, event3.
        commit_position3_1 = await self.client.multi_append_to_stream(
            NewEvents(
                stream_name,
                current_version=0,
                events=[event2, event3],
            )
        )
        self.assertEqual(commit_position3, commit_position3_1)

        # Stream should still have 3 events.
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Mixture of "idempotent" write of event2, event3, with new event4.
        with self.assertRaises(WrongCurrentVersionError):
            await self.client.multi_append_to_stream(
                NewEvents(
                    stream_name,
                    current_version=0,
                    events=[event2, event3, event4],
                )
            )

        # Stream should still have 3 events.
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Append events with same ID at end of stream (specify current version).
        await self.client.multi_append_to_stream(
            NewEvents(stream_name, [event2, event3, event4], 2)
        )

        # Stream now has 6 events....
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

        # Append events with same ID at end of stream (specify stream exists).
        await self.client.multi_append_to_stream(
            NewEvents(stream_name, [event2, event1], StreamState.EXISTS)
        )

        # Stream still has 6 events....
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

        # Append events with same ID at end of stream (disable OCC).
        await self.client.multi_append_to_stream(
            NewEvents(stream_name, [event2, event1], StreamState.ANY)
        )

        # Stream still has 6 events....
        events = await self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_many_streams(self) -> None:
        stream_name1 = str(uuid4())
        stream_name2 = str(uuid4())

        # Construct four new events.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        event2 = NewEvent(
            type="OrderUpdated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        event3 = NewEvent(
            type="OrderDeleted",
            data=random_data(),
            content_type="application/octet-stream",
        )
        event4 = NewEvent(
            type="OrderCorrected",
            data=random_data(),
            content_type="application/octet-stream",
        )

        # Append one event each to two streams.
        await self.client.multi_append_to_stream(
            [
                NewEvents(stream_name1, [event1], StreamState.NO_STREAM),
                NewEvents(stream_name2, [event2], StreamState.NO_STREAM),
            ]
        )

        # Expect each stream has one event.
        events = await self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        events = await self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Idempotent append.
        await self.client.multi_append_to_stream(
            [
                NewEvents(stream_name1, [event1], StreamState.NO_STREAM),
                NewEvents(stream_name2, [event2], StreamState.NO_STREAM),
            ]
        )

        # Expect each stream still has one event.
        events = await self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        events = await self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Append errors.
        with self.assertRaises(WrongCurrentVersionError):
            await self.client.multi_append_to_stream(
                [
                    NewEvents(stream_name1, [event3], StreamState.NO_STREAM),
                    NewEvents(stream_name2, [event4], StreamState.NO_STREAM),
                ]
            )

        with self.assertRaises(WrongCurrentVersionError):
            await self.client.multi_append_to_stream(
                [
                    NewEvents(stream_name1, [event3], 0),
                    NewEvents(stream_name2, [event4], StreamState.NO_STREAM),
                ]
            )

        with self.assertRaises(WrongCurrentVersionError):
            await self.client.multi_append_to_stream(
                [
                    NewEvents(stream_name1, [event3], StreamState.NO_STREAM),
                    NewEvents(stream_name2, [event4], 0),
                ]
            )

        # Expect each stream still has one event.
        events = await self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        events = await self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Correct append to existing multi-streams.
        await self.client.multi_append_to_stream(
            [
                NewEvents(stream_name1, [event3], 0),
                NewEvents(stream_name2, [event4], 0),
            ]
        )

        # Expect each stream now has two events.
        events = await self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event3.id)

        events = await self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event4.id)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_wrong_credentials(self) -> None:
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        with self.assertRaises(UnauthenticatedError):
            await self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event1],
                    current_version=StreamState.NO_STREAM,
                ),
                credentials=self.client.construct_call_credentials("foo", "assword"),
            )

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_stream_already_exists_error(self) -> None:
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        await self.client.multi_append_to_stream(
            events=NewEvents(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            ),
        )
        # This doesn't fail because its treated as idempotent.
        await self.client.multi_append_to_stream(
            events=NewEvents(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            ),
        )
        # We need a different event.
        event2 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        # TODO: Thought this might give StreamAlreadyExistsErrorDetails
        #  but get StreamRevisionConflictErrorDetails instead.
        #  - What gives StreamAlreadyExistsErrorDetails?
        with self.assertRaises(WrongCurrentVersionError) as cm:
            await self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event2],
                    current_version=StreamState.NO_STREAM,
                ),
            )
        self.assertEqual(cm.exception.stream_name, stream_name1)
        self.assertEqual(cm.exception.current_version, 0)
        self.assertEqual(cm.exception.expected_version, -1)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_same_stream_error(self) -> None:
        stream_name1 = str(uuid4())

        # Construct a new event.

        with self.assertRaises(MultiAppendToSameStreamError) as cm:

            while True:
                await self.client.multi_append_to_stream(
                    events=[
                        NewEvents(
                            stream_name=stream_name1,
                            events=[
                                NewEvent(
                                    type="OrderCreated",
                                    data=random_data(),
                                    content_type="application/octet-stream",
                                )
                            ],
                            current_version=StreamState.ANY,
                        ),
                        NewEvents(
                            stream_name=stream_name1,
                            events=[
                                NewEvent(
                                    type="OrderCreated",
                                    data=random_data(),
                                    content_type="application/octet-stream",
                                )
                            ],
                            current_version=StreamState.ANY,
                        ),
                    ],
                )

        self.assertEqual(cm.exception.stream_name, stream_name1)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_tombstoned_stream_error(self) -> None:
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        await self.client.multi_append_to_stream(
            events=NewEvents(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            ),
        )

        await self.client.tombstone_stream(
            stream_name1, current_version=StreamState.EXISTS
        )

        with self.assertRaises(StreamTombstonedError):
            await self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event1],
                    current_version=StreamState.ANY,
                ),
            )

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    async def test_stream_multi_append_metadata_conversions_and_errors(self) -> None:
        stream_name = str(uuid4())

        async def append_helper(metadata: bytes) -> None:
            self.assertIsInstance(metadata, bytes)
            event = NewEvent(
                type="OrderCreated",
                data=random_data(),
                metadata=metadata,
                content_type="application/octet-stream",
            )
            await self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name,
                    events=[event],
                    current_version=StreamState.ANY,
                ),
            )

        # These are OK.
        await append_helper(b"")
        await append_helper(json.dumps({"a": "1"}).encode())

        # These are not OK.
        with self.assertRaises(ProgrammingError):
            await append_helper(random_data(100))
        with self.assertRaises(ProgrammingError):
            await append_helper(json.dumps("a").encode())
        with self.assertRaises(ProgrammingError):
            await append_helper(json.dumps({"a": 1}).encode())
        with self.assertRaises(ProgrammingError):
            await append_helper(json.dumps({"a": 1}).encode())
        with self.assertRaises(ProgrammingError):
            await append_helper(json.dumps({"a": {}}).encode())

        events = await self.client.get_stream(stream_name)
        self.assertEqual(2, len(events))
        self.assertEqual(
            json.loads(events[0].metadata.decode()),
            {"$schema.format": "Bytes", "$schema.name": "OrderCreated"},
        )
        self.assertEqual(
            json.loads(events[1].metadata.decode()),
            {"$schema.format": "Bytes", "$schema.name": "OrderCreated", "a": "1"},
        )

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support secondary indexes")
    async def test_read_index(self) -> None:
        stream_name1 = str(uuid4())
        event_type = f"OrderCreated{uuid4()!s}"

        # Construct a new event.
        event1 = NewEvent(
            type=event_type,
            data=random_data(),
            content_type="application/octet-stream",
        )

        await self.client.append_to_stream(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        # Index is eventually consistent, so need retries.
        retry_count = 5
        while retry_count:
            read_response = await self.client.read_index(f"et-{event_type}")
            if len([e async for e in read_response]):
                break
            await asyncio.sleep(1)
            retry_count -= 1
        else:
            self.fail("Failed to read event from index")

        # Do it again with "$idx-" prefix - don't need to wait this time.
        read_response = await self.client.read_index(f"$idx-et-{event_type}")
        self.assertEqual(1, len([e async for e in read_response]))

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support secondary indexes")
    async def test_subscribe_to_index(self) -> None:
        stream_name1 = str(uuid4())
        event_type = f"OrderCreated{uuid4()!s}"

        # Construct a new event.
        event1 = NewEvent(
            type=event_type,
            data=random_data(),
            content_type="application/octet-stream",
        )

        await self.client.append_to_stream(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        async with await self.client.subscribe_to_index(
            f"et-{event_type}"
        ) as subscription:
            async for event in subscription:
                if event.type == event_type:
                    break
                self.fail("Failed to read event from index")

        # Do it again with "$idx-" prefix.
        async with await self.client.subscribe_to_index(
            f"$idx-et-{event_type}"
        ) as subscription:
            async for event in subscription:
                if event.type == event_type:
                    break
                self.fail("Failed to read event from index")


class TestOptionalClientAuth(TimedTestCase, IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.user_key = b"some-key"
        self.user_cert = b"some-cert"
        self.tls_ca = b"some-cert"
        with (
            NamedTemporaryFile(delete=False) as f1,
            NamedTemporaryFile(delete=False) as f2,
            NamedTemporaryFile(delete=False) as f3,
        ):
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
        uri = f"kdb://admin:changeit@{secure_grpc_target}"

        # Construct client without client auth.
        client = AsyncKurrentDBClient(uri, root_certificates=root_certificates)
        await client.connect()

        # User key and cert should be None.
        self.assertIsNone(client.private_key)
        self.assertIsNone(client.certificate_chain)

        # Should be able to get commit position.
        await client.get_commit_position()

        # Construct client with client auth.
        uri += f"?UserKeyFile={self.user_key_file}&UserCertFile={self.user_cert_file}"
        client = AsyncKurrentDBClient(uri, root_certificates=root_certificates)
        await client.connect()

        # User cert and key should have expected values.
        self.assertEqual(self.user_key, client.private_key)
        self.assertEqual(self.user_cert, client.certificate_chain)

        # Should raise SSL error.
        with self.assertRaises(SSLError):
            await client.get_commit_position()

        # Construct client with TlsCaFile (instead
        # of passing root_certificates directly).
        uri += f"&TlsCaFile={self.tls_ca_file}"
        client_with_tls_ca = AsyncKurrentDBClient(uri)
        await client_with_tls_ca.connect()

        # Read the contents of TlsCaFile as bytes,
        # because root_certificates are compared as bytes.
        with open(self.tls_ca_file, "rb") as f:  # noqa: ASYNC101
            tls_ca_file_contents = f.read()

        # TlsCaFile should override the root_certificates passed directly.
        self.assertNotEqual(root_certificates, client_with_tls_ca.root_certificates)
        self.assertEqual(tls_ca_file_contents, client_with_tls_ca.root_certificates)
