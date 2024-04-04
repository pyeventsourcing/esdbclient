# -*- coding: utf-8 -*-
import asyncio
import sys
from typing import Optional

from esdbclient.persistent import AsyncioSubscriptionReadReqs
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
    ExceptionIteratingRequests,
    FollowerNotFound,
    GrpcDeadlineExceeded,
    NodeIsNotLeader,
    NotFound,
    ProgrammingError,
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
            def __init__(self, client: _AsyncioEventStoreDBClient) -> None:
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
        await self.client.close()
        catchup_subscription = await self.client.subscribe_to_all()
        self.assertIsInstance(catchup_subscription, AsyncioCatchupSubscription)

        # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
        self.client._connection = self.client._construct_esdb_connection(
            "localhost:22222"
        )
        catchup_subscription = await self.client.subscribe_to_all()
        self.assertIsInstance(catchup_subscription, AsyncioCatchupSubscription)

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
        # print("Event1: ", event1.id)
        await self.client.append_events(
            stream_name=stream_name1,
            events=[event1],
            current_version=StreamState.NO_STREAM,
        )

        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated2", data=b"{}")
        #         print("Event2: ", event2.id)
        await self.client.append_events(
            stream_name=stream_name2,
            events=[event2],
            current_version=StreamState.NO_STREAM,
        )

        # Read subscription - error iterating requests is propagated.
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        with self.assertRaises(ExceptionIteratingRequests):
            async for event in persistent_subscription:
                #                 print(event.id, event.retry_count, event.recorded_at)
                await persistent_subscription.ack("a")  # type: ignore[arg-type]
                # await persistent_subscription.ack(event)
                # if event.id == event2.id:
                #     await persistent_subscription.stop()

        # Read subscription - success.
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        events = []
        async for event in persistent_subscription:
            #             print(event.id, event.retry_count, event.recorded_at)
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
        #         print("Event3: ", event3.id)
        await self.client.append_events(
            stream_name=stream_name3,
            events=[event3],
            current_version=StreamState.NO_STREAM,
        )
        stream_name4 = str(uuid4())
        event4 = NewEvent(type="OrderCreated4", data=b"{}")
        #         print("Event4: ", event4.id)
        await self.client.append_events(
            stream_name=stream_name4,
            events=[event4],
            current_version=StreamState.NO_STREAM,
        )
        # - retry events
        #         print("Nack with retry")
        events = []
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        async for event in persistent_subscription:
            #             print(event.id, event.retry_count, event.recorded_at)
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
        #         print("Nack with park")
        events = []
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        async for event in persistent_subscription:
            #             print(event.id, event.retry_count, event.recorded_at)
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
        #         print("Sleeping")
        await asyncio.sleep(1)
        #         print("Blah blah blah")
        events = []
        persistent_subscription = await self.client.read_subscription_to_all(group_name)
        async for event in persistent_subscription:
            #             print(event.id, event.retry_count, event.recorded_at)
            events.append(event)
            await persistent_subscription.ack(event)
            if event.id == event4.id:
                # break
                await persistent_subscription.stop()
        self.assertEqual(len(events), 2)
        self.assertEqual(events[-2].id, event3.id)
        self.assertEqual(events[-1].id, event4.id)

        # Get subscription info.
        info = await self.client.get_subscription_info(group_name)
        self.assertEqual(info.group_name, group_name)
        self.assertFalse(info.resolve_link_tos)

        # Update subscription.
        await self.client.update_subscription_to_all(
            group_name=group_name, resolve_links=True
        )
        info = await self.client.get_subscription_info(group_name)
        self.assertTrue(info.resolve_link_tos)

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
                #                 print(event.id, event.retry_count, event.recorded_at)
                await subscription.ack("a")  # type: ignore[arg-type]
                # await persistent_subscription.ack(event)
                # if event.id == event2.id:
                #     await persistent_subscription.stop()

        # Read subscription - success.
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        events = []
        async for event in subscription:
            #             print(event.id, event.retry_count, event.recorded_at)
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
            #             print(event.id, event.retry_count, event.recorded_at)
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
        #         print("Nack with retry")
        events = []
        subscription = await self.client.read_subscription_to_stream(
            group_name, stream_name1
        )
        async for event in subscription:
            #             print(event.id, event.retry_count, event.recorded_at)
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
        self.assertFalse(info.resolve_link_tos)

        # Update subscription.
        await self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name1, resolve_links=True
        )
        info = await self.client.get_subscription_info(group_name, stream_name1)
        self.assertTrue(info.resolve_link_tos)

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
        self.assertTrue(s._is_stopped)
        await s.stop()
        self.assertTrue(s._is_stopped)

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

    async def test_persistent_subscription_sends_acks(self) -> None:
        reqs = AsyncioSubscriptionReadReqs("group1", max_ack_batch_size=3)
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

    async def test_persistent_subscription_context_manager(self) -> None:
        group_name = str(uuid4())
        await self.client._connection.close()
        await self.client.create_subscription_to_all(group_name)
        s = await self.client.read_subscription_to_all(group_name)
        async with s as s:
            pass
        self.assertTrue(s._is_stopped)

    # async def test_subscribe_to_all_raises_discovery_failed(self) -> None:
    #     await self.client._connection.close()
    #     # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
    #     await self.client._connection.close()
    #     self.client._connection = self.client._construct_esdb_connection("localhost:2222")
    #
    #     await self.client.subscribe_to_all()
    #     # with self.assertRaises(ServiceUnavailable):
