# -*- coding: utf-8 -*-
import datetime
import os
import ssl
import sys
import traceback
from collections import Counter
from threading import Thread
from time import sleep
from typing import Any, List, Sequence, Set, Tuple, cast
from unittest import TestCase, skipIf
from unittest.case import _AssertRaisesContext
from uuid import UUID, uuid4

from grpc import RpcError, StatusCode
from grpc._channel import _MultiThreadedRendezvous, _RPCState
from grpc._cython.cygrpc import IntegratedCall

import esdbclient.protos.Grpc.persistent_pb2 as grpc_persistent
from esdbclient import ESDB_SYSTEM_EVENTS_REGEX, RecordedEvent, StreamState
from esdbclient.client import EventStoreDBClient
from esdbclient.common import handle_rpc_error
from esdbclient.connection_spec import (
    NODE_PREFERENCE_FOLLOWER,
    NODE_PREFERENCE_LEADER,
    ConnectionSpec,
)
from esdbclient.events import Checkpoint, NewEvent
from esdbclient.exceptions import (
    AbortedByServer,
    AlreadyExists,
    ConsumerTooSlow,
    DiscoveryFailed,
    DNSError,
    ExceptionIteratingRequests,
    ExceptionThrownByHandler,
    FollowerNotFound,
    GossipSeedError,
    GrpcDeadlineExceeded,
    GrpcError,
    MaximumSubscriptionsReached,
    NodeIsNotLeader,
    NotFound,
    ReadOnlyReplicaNotFound,
    ServiceUnavailable,
    StreamIsDeleted,
    WrongCurrentVersion,
)
from esdbclient.gossip import NODE_STATE_FOLLOWER, NODE_STATE_LEADER
from esdbclient.persistent import SubscriptionReadReqs
from esdbclient.protos.Grpc import persistent_pb2

started = datetime.datetime.now()
last = datetime.datetime.now()

EVENTSTORE_IMAGE_TAG = os.environ.get("EVENTSTORE_IMAGE_TAG", "")


def get_elapsed_time() -> str:
    global last
    last = datetime.datetime.now()
    delta = last - started
    result = ""
    minutes = int(delta.seconds // 60)
    if minutes:
        result += f"{minutes}m"
    seconds = int(delta.seconds % 60)
    return result + f"{seconds}s"


def get_duration() -> str:
    delta_seconds = (datetime.datetime.now() - last).total_seconds()
    minutes = int(delta_seconds // 60)
    result = ""
    if minutes:
        result = f"{minutes}m"
    seconds = delta_seconds % 60
    return result + f"{seconds:.3f}s"


class TimedTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        if "-v" in sys.argv:
            sys.stderr.write(f"[@{get_elapsed_time()}] ")
            sys.stderr.flush()

    def tearDown(self) -> None:
        if "-v" in sys.argv:
            sys.stderr.write(f"[+{get_duration()}] ")
        super().tearDown()


class TestConnectionSpec(TestCase):
    def test_scheme_and_netloc(self) -> None:
        spec = ConnectionSpec("esdb://host1:2110")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "host1:2110")
        self.assertEqual(spec.targets, ["host1:2110"])
        self.assertEqual(spec.username, None)
        self.assertEqual(spec.password, None)

        spec = ConnectionSpec("esdb://admin:changeit@host1:2110")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "admin:changeit@host1:2110")
        self.assertEqual(spec.targets, ["host1:2110"])
        self.assertEqual(spec.username, "admin")
        self.assertEqual(spec.password, "changeit")

        spec = ConnectionSpec("esdb://host1:2110,host2:2111,host3:2112")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "host1:2110,host2:2111,host3:2112")
        self.assertEqual(spec.targets, ["host1:2110", "host2:2111", "host3:2112"])
        self.assertEqual(spec.username, None)
        self.assertEqual(spec.password, None)

        spec = ConnectionSpec("esdb://admin:changeit@host1:2110,host2:2111,host3:2112")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "admin:changeit@host1:2110,host2:2111,host3:2112")
        self.assertEqual(spec.targets, ["host1:2110", "host2:2111", "host3:2112"])
        self.assertEqual(spec.username, "admin")
        self.assertEqual(spec.password, "changeit")

        spec = ConnectionSpec("esdb+discover://host1:2110")
        self.assertEqual(spec.scheme, "esdb+discover")
        self.assertEqual(spec.netloc, "host1:2110")
        self.assertEqual(spec.targets, ["host1:2110"])
        self.assertEqual(spec.username, None)
        self.assertEqual(spec.password, None)

        spec = ConnectionSpec("esdb+discover://admin:changeit@host1:2110")
        self.assertEqual(spec.scheme, "esdb+discover")
        self.assertEqual(spec.netloc, "admin:changeit@host1:2110")
        self.assertEqual(spec.targets, ["host1:2110"])
        self.assertEqual(spec.username, "admin")
        self.assertEqual(spec.password, "changeit")

    def test_tls(self) -> None:
        # Tls not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertIs(spec.options.Tls, True)

        # Set Tls "true".
        spec = ConnectionSpec("esdb:?Tls=true")
        self.assertIs(spec.options.Tls, True)

        # Set Tls "false".
        spec = ConnectionSpec("esdb:?Tls=false")
        self.assertIs(spec.options.Tls, False)

        # Check case insensitivity.
        spec = ConnectionSpec("esdb:?TLS=false")
        self.assertIs(spec.options.Tls, False)
        spec = ConnectionSpec("esdb:?tls=false")
        self.assertIs(spec.options.Tls, False)
        spec = ConnectionSpec("esdb:?TLS=true")
        self.assertIs(spec.options.Tls, True)
        spec = ConnectionSpec("esdb:?tls=true")
        self.assertIs(spec.options.Tls, True)

        spec = ConnectionSpec("esdb:?TLS=False")
        self.assertIs(spec.options.Tls, False)
        spec = ConnectionSpec("esdb:?tls=FALSE")
        self.assertIs(spec.options.Tls, False)

        # Invalid value.
        with self.assertRaises(ValueError):
            ConnectionSpec("esdb:?Tls=blah")

        # Repeated field (use first value).
        spec = ConnectionSpec("esdb:?Tls=true&Tls=false")
        self.assertTrue(spec.options.Tls)

    def test_connection_name(self) -> None:
        # ConnectionName not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertIsInstance(spec.options.ConnectionName, str)

        # Set ConnectionName.
        connection_name = str(uuid4())
        spec = ConnectionSpec(f"esdb:?ConnectionName={connection_name}")
        self.assertEqual(spec.options.ConnectionName, connection_name)

        # Check case insensitivity.
        spec = ConnectionSpec(f"esdb:?connectionName={connection_name}")
        self.assertEqual(spec.options.ConnectionName, connection_name)

        # Check case insensitivity.
        spec = ConnectionSpec(f"esdb:?connectionName={connection_name}")
        self.assertEqual(spec.options.ConnectionName, connection_name)

    def test_max_discover_attempts(self) -> None:
        # MaxDiscoverAttempts not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.MaxDiscoverAttempts, 10)

        # Set MaxDiscoverAttempts.
        spec = ConnectionSpec("esdb:?MaxDiscoverAttempts=5")
        self.assertEqual(spec.options.MaxDiscoverAttempts, 5)

    def test_discovery_interval(self) -> None:
        # DiscoveryInterval not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.DiscoveryInterval, 100)

        # Set DiscoveryInterval.
        spec = ConnectionSpec("esdb:?DiscoveryInterval=200")
        self.assertEqual(spec.options.DiscoveryInterval, 200)

    def test_gossip_timeout(self) -> None:
        # GossipTimeout not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.GossipTimeout, 5)

        # Set GossipTimeout.
        spec = ConnectionSpec("esdb:?GossipTimeout=10")
        self.assertEqual(spec.options.GossipTimeout, 10)

    def test_node_preference(self) -> None:
        # NodePreference not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_LEADER)

        # Set NodePreference.
        spec = ConnectionSpec("esdb:?NodePreference=leader")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec("esdb:?NodePreference=follower")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_FOLLOWER)

        # Invalid value.
        with self.assertRaises(ValueError):
            ConnectionSpec("esdb:?NodePreference=blah")

        # Case insensitivity.
        spec = ConnectionSpec("esdb:?nodePreference=leader")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec("esdb:?NODEPREFERENCE=follower")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_FOLLOWER)
        spec = ConnectionSpec("esdb:?NodePreference=Leader")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec("esdb:?NodePreference=FOLLOWER")
        self.assertEqual(spec.options.NodePreference, NODE_PREFERENCE_FOLLOWER)

    def test_tls_verify_cert(self) -> None:
        # TlsVerifyCert not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.TlsVerifyCert, True)

        # Set TlsVerifyCert.
        spec = ConnectionSpec("esdb:?TlsVerifyCert=true")
        self.assertEqual(spec.options.TlsVerifyCert, True)
        spec = ConnectionSpec("esdb:?TlsVerifyCert=false")
        self.assertEqual(spec.options.TlsVerifyCert, False)

        # Invalid value.
        with self.assertRaises(ValueError):
            ConnectionSpec("esdb:?TlsVerifyCert=blah")

        # Case insensitivity.
        spec = ConnectionSpec("esdb:?TLSVERIFYCERT=true")
        self.assertEqual(spec.options.TlsVerifyCert, True)
        spec = ConnectionSpec("esdb:?tlsverifycert=false")
        self.assertEqual(spec.options.TlsVerifyCert, False)
        spec = ConnectionSpec("esdb:?TlsVerifyCert=True")
        self.assertEqual(spec.options.TlsVerifyCert, True)
        spec = ConnectionSpec("esdb:?TlsVerifyCert=False")
        self.assertEqual(spec.options.TlsVerifyCert, False)

    def test_default_deadline(self) -> None:
        # DefaultDeadline not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.DefaultDeadline, None)

        # Set DefaultDeadline.
        spec = ConnectionSpec("esdb:?DefaultDeadline=10")
        self.assertEqual(spec.options.DefaultDeadline, 10)

    def test_keep_alive_interval(self) -> None:
        # KeepAliveInterval not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.KeepAliveInterval, None)

        # Set KeepAliveInterval.
        spec = ConnectionSpec("esdb:?KeepAliveInterval=10")
        self.assertEqual(spec.options.KeepAliveInterval, 10)

    def test_keep_alive_timeout(self) -> None:
        # KeepAliveTimeout not mentioned.
        spec = ConnectionSpec("esdb:")
        self.assertEqual(spec.options.KeepAliveTimeout, None)

        # Set KeepAliveTimeout.
        spec = ConnectionSpec("esdb:?KeepAliveTimeout=10")
        self.assertEqual(spec.options.KeepAliveTimeout, 10)

    def test_raises_when_query_string_has_unsupported_field(self) -> None:
        with self.assertRaises(ValueError) as cm1:
            ConnectionSpec("esdb:?NotSupported=10")
        self.assertIn("Unknown field in", cm1.exception.args[0])

        with self.assertRaises(ValueError) as cm2:
            ConnectionSpec("esdb:?NotSupported=10&AlsoNotSupported=20")
        self.assertIn("Unknown fields in", cm2.exception.args[0])


def get_ca_certificate() -> str:
    ca_cert_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "certs/ca/ca.crt"
    )
    with open(ca_cert_path, "r") as f:
        return f.read()


def get_server_certificate(grpc_target: str) -> str:
    return ssl.get_server_certificate(
        addr=cast(Tuple[str, int], grpc_target.split(":")),
    )


class TestEventStoreDBClient(TimedTestCase):
    client: EventStoreDBClient

    ESDB_TARGET = "localhost:2114"
    ESDB_TLS = True
    ESDB_CLUSTER_SIZE = 1

    def construct_esdb_client(self) -> None:
        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        if self.ESDB_TLS:
            uri = f"esdb://admin:changeit@{self.ESDB_TARGET}?{qs}"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"esdb://{self.ESDB_TARGET}?Tls=false&{qs}"
            root_certificates = None
        self.client = EventStoreDBClient(uri, root_certificates=root_certificates)

    def get_root_certificates(self) -> str:
        if self.ESDB_CLUSTER_SIZE == 1:
            return get_server_certificate(self.ESDB_TARGET)
        elif self.ESDB_CLUSTER_SIZE == 3:
            return get_ca_certificate()
        else:
            raise ValueError(
                f"Test doesn't work with cluster size {self.ESDB_CLUSTER_SIZE}"
            )

    def tearDown(self) -> None:
        super().tearDown()
        if hasattr(self, "client"):
            self.client.close()

    def test_close(self) -> None:
        self.construct_esdb_client()
        self.client.close()
        self.client.close()

        self.construct_esdb_client()
        self.client.close()
        self.client.close()

    def test_constructor_with_tls_true_but_no_root_certificates(self) -> None:
        if self.ESDB_TLS:
            qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
            uri = f"esdb://admin:changeit@{self.ESDB_TARGET}?{qs}"
            try:
                EventStoreDBClient(uri)
            except DiscoveryFailed:
                tb = traceback.format_exc()
                self.assertIn("Ssl handshake failed", tb)
                self.assertIn("routines:OPENSSL_internal:CERTIFICATE_VERIFY_FAILED", tb)
            else:
                self.fail("Didn't raise DiscoveryFailed exception")
        else:
            self.skipTest("Skipping for insecure server")

    def test_constructor_raises_value_errors(self) -> None:
        # Secure URI without username or password.
        with self.assertRaises(ValueError) as cm0:
            EventStoreDBClient("esdb://localhost:2222")
        self.assertIn(
            "username and password are required",
            cm0.exception.args[0],
        )

        # Scheme must be 'esdb'.
        with self.assertRaises(ValueError) as cm1:
            EventStoreDBClient(uri="http://localhost:2222")
        self.assertIn("Invalid URI scheme:", cm1.exception.args[0])

    def test_constructor_raises_gossip_seed_error(self) -> None:
        # Needs at least one target.
        with self.assertRaises(GossipSeedError):
            EventStoreDBClient(uri="esdb://")

    def test_raises_discovery_failed_exception(self) -> None:
        self.construct_esdb_client()

        # Reconstruct connection with wrong port.
        self.client._esdb.close()
        self.client._esdb = self.client._construct_esdb_connection("localhost:2222")
        self.client.connection_spec._targets = ["localhost:2222"]

        cm: _AssertRaisesContext[Any]

        with self.assertRaises(DiscoveryFailed):
            self.client.get_stream(str(uuid4()))

        # Todo: Maybe other methods here...?

        # Reconstruct client with wrong port.
        esdb_target = "localhost:2222"

        qs = "MaxDiscoverAttempts=2&DiscoveryInterval=0&GossipTimeout=1"
        if self.ESDB_TLS:
            uri = f"esdb://admin:changeit@{esdb_target}?{qs}"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"esdb://{esdb_target}?Tls=false&{qs}"
            root_certificates = None

        with self.assertRaises(DiscoveryFailed):
            EventStoreDBClient(uri, root_certificates=root_certificates)

    def test_constructor_connects_despite_bad_target_in_gossip_seed(self) -> None:
        # Reconstruct connection with wrong port.
        esdb_target = f"localhost:2222,{self.ESDB_TARGET}"

        if self.ESDB_TLS:
            uri = f"esdb://admin:changeit@{esdb_target}"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"esdb://{esdb_target}?Tls=false"
            root_certificates = None

        try:
            client = EventStoreDBClient(uri, root_certificates=root_certificates)
        except Exception:
            self.fail("Failed to connect")
        else:
            client.close()

    def test_stream_read_raises_not_found(self) -> None:
        # Note, we never get a NotFound from subscribe_to_stream(), which is
        # logical because the stream might be written after the subscription. So here
        # we just test get_stream().

        self.construct_esdb_client()
        stream_name = str(uuid4())

        read_response = self.client.read_stream(stream_name)
        with self.assertRaises(NotFound):
            tuple(read_response)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name, backwards=True)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name, stream_position=1)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name, stream_position=1, backwards=True)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name, limit=10)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name, backwards=True, limit=10)

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name, stream_position=1, limit=10)

        with self.assertRaises(NotFound):
            self.client.get_stream(
                stream_name, stream_position=1, backwards=True, limit=10
            )

    def test_stream_append_to_stream(self) -> None:
        # This method exists to match other language clients.
        self.construct_esdb_client()
        stream_name = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append single event.
        commit_position1 = self.client.append_to_stream(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=event1,
        )

        # Append sequence of events.
        commit_position2 = self.client.append_to_stream(
            stream_name=stream_name,
            current_version=0,
            events=[event2, event3],
        )

        # Check commit positions are returned.
        events = list(self.client.read_all(commit_position=commit_position1))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].commit_position, commit_position1)
        self.assertEqual(events[2].commit_position, commit_position2)

    def test_stream_append_to_stream_takes_one_or_many_events(self) -> None:
        # This method exists to match other language clients.
        self.construct_esdb_client()
        stream_name = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append single event.
        commit_position1 = self.client.append_to_stream(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=event1,
        )

        # Append sequence of events.
        commit_position2 = self.client.append_to_stream(
            stream_name=stream_name,
            current_version=0,
            events=[event2, event3],
        )

        # Check commit positions are returned.
        events = list(self.client.read_all(commit_position=commit_position1))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].commit_position, commit_position1)
        self.assertEqual(events[2].commit_position, commit_position2)

    # def test_stream_append_to_stream_is_atomic(self) -> None:
    #     # This method exists to match other language clients.
    #     self.construct_esdb_client()
    #
    #     def append_events(stream_name: str, n: int) -> bool:
    #         timeout = None
    #         # print("Generating events")
    #         events = list(
    #             [NewEvent(type=f"EventType{i}", data=b"{}") for i in range(n)]
    #         )
    #
    #         # print("Appending events")
    #         try:
    #             self.client.append_to_stream(
    #                 stream_name=stream_name,
    #                 current_version=StreamState.NO_STREAM,
    #                 events=events,
    #                 timeout=timeout,
    #             )
    #         except GrpcError as e:
    #             # print("Error appending events:", e)
    #             return False
    #         else:
    #             return True
    #
    #     def count_events(stream_name: str) -> int:
    #         try:
    #             return len(self.client.get_stream(stream_name))
    #         except NotFound:
    #             # print("Not found")
    #             return 0
    #
    #     # Should be able to append 2 events.
    #     stream_name1 = str(uuid4())
    #     self.assertTrue(append_events(stream_name1, 2))
    #     self.assertTrue(count_events(stream_name1))
    #
    #     # Should NOT be able to append 10000 events.
    #     stream_name2 = str(uuid4())
    #     self.assertFalse(append_events(stream_name2, 20000))
    #     self.assertFalse(count_events(stream_name2))  # should be atomic

    def test_stream_append_event_with_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Check stream position is None.
        self.assertEqual(
            self.client.get_current_version(stream_name), StreamState.NO_STREAM
        )

        # Todo: Reintroduce this when/if testing for streaming individual events.
        # # Check get error when attempting to append empty list to position 1.
        # with self.assertRaises(ExpectedPositionError) as cm:
        #     self.client.append_events(stream_name, current_version=1, events=[])
        # self.assertEqual(cm.exception.args[0], f"Stream {stream_name!r} does not exist")

        # # Append empty list of events.
        # commit_position0 = self.client.append_events(
        #     stream_name, current_version=StreamState.NO_STREAM, events=[]
        # )
        # self.assertIsInstance(commit_position0, int)

        # # Check stream still not found.
        # with self.assertRaises(NotFound):
        #     self.client.get_stream(stream_name)

        # # Check stream position is None.
        # self.assertEqual(self.client.get_current_version(stream_name), None)

        # Construct three new events.
        event1 = NewEvent(
            type="OrderCreated", data=random_data(), metadata=random_data()
        )
        event2 = NewEvent(
            type="OrderUpdated", data=random_data(), metadata=random_data()
        )
        event3 = NewEvent(
            type="OrderDeleted", data=random_data(), metadata=random_data()
        )
        event4 = NewEvent(
            type="OrderCorrected", data=random_data(), metadata=random_data()
        )

        # Check get error when attempting to append new event to position 1.
        with self.assertRaises(WrongCurrentVersion) as cm:
            self.client.append_event(stream_name, current_version=1, event=event1)
        self.assertEqual(cm.exception.args[0], f"Stream {stream_name!r} does not exist")

        # Append new event with correct expected position of 'None'.
        commit_position0 = self.client.get_commit_position()
        commit_position1 = self.client.append_event(
            stream_name, current_version=StreamState.NO_STREAM, event=event1
        )

        # Check commit position is greater.
        self.assertGreater(commit_position1, commit_position0)

        # Check stream position is 0.
        self.assertEqual(self.client.get_current_version(stream_name), 0)

        # Read the stream forwards from the start (expect one event).
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 1)

        # Check the attributes of the recorded event.
        self.assertEqual(events[0].type, event1.type)
        self.assertEqual(events[0].data, event1.data)
        self.assertEqual(events[0].metadata, event1.metadata)
        self.assertEqual(events[0].content_type, event1.content_type)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[0].stream_name, stream_name)
        self.assertEqual(events[0].stream_position, 0)
        if events[0].commit_position is not None:  # v21.10 doesn't return this
            self.assertEqual(events[0].commit_position, commit_position1)

        # Check we can't append another new event at initial position.

        with self.assertRaises(WrongCurrentVersion) as cm:
            self.client.append_event(
                stream_name, current_version=StreamState.NO_STREAM, event=event2
            )
        self.assertEqual(cm.exception.args[0], "Current version is 0")

        # Append another event.
        commit_position2 = self.client.append_event(
            stream_name, current_version=0, event=event2
        )

        # Check stream position is 1.
        self.assertEqual(self.client.get_current_version(stream_name), 1)

        # Check stream position.
        self.assertGreater(commit_position2, commit_position1)

        # Read the stream (expect two events in 'forwards' order).
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

        # Read the stream backwards from the end.
        events = self.client.get_stream(stream_name, backwards=True)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event1.id)

        # Read the stream forwards from position 1.
        events = self.client.get_stream(stream_name, stream_position=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Read the stream backwards from position 0.
        events = self.client.get_stream(stream_name, stream_position=0, backwards=True)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Read the stream forwards from start with limit.
        events = self.client.get_stream(stream_name, limit=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Read the stream backwards from end with limit.
        events = self.client.get_stream(stream_name, backwards=True, limit=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Check we can't append another new event at second position.
        with self.assertRaises(WrongCurrentVersion) as cm:
            self.client.append_event(stream_name, current_version=0, event=event3)
        self.assertEqual(cm.exception.args[0], "Current version is 1")

        # Append another new event.
        commit_position3 = self.client.append_event(
            stream_name, current_version=1, event=event3
        )

        # Check stream position is 2.
        self.assertEqual(self.client.get_current_version(stream_name), 2)

        # Check the commit position.
        self.assertGreater(commit_position3, commit_position2)

        # Read the stream forwards from start (expect three events).
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Read the stream backwards from end (expect three events).
        events = self.client.get_stream(stream_name, backwards=True)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event1.id)

        # Read the stream forwards from position 1 with limit 1.
        events = self.client.get_stream(stream_name, stream_position=1, limit=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Read the stream backwards from position 1 with limit 1.
        events = self.client.get_stream(
            stream_name, stream_position=1, backwards=True, limit=1
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Idempotent write of event1.
        commit_position1_1 = self.client.append_event(
            stream_name, current_version=StreamState.NO_STREAM, event=event1
        )
        self.assertEqual(commit_position1, commit_position1_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event2.
        commit_position2_1 = self.client.append_event(
            stream_name, current_version=0, event=event2
        )
        self.assertEqual(commit_position2_1, commit_position2)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event3.
        commit_position3_1 = self.client.append_event(
            stream_name, current_version=1, event=event3
        )
        self.assertEqual(commit_position3, commit_position3_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event1, event2.
        commit_position2_1 = self.client.append_events(
            stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )
        self.assertEqual(commit_position2, commit_position2_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event2, event3.
        commit_position3_1 = self.client.append_events(
            stream_name,
            current_version=0,
            events=[event2, event3],
        )
        self.assertEqual(commit_position3, commit_position3_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Mixture of "idempotent" write of event2, event3, with new event4.
        with self.assertRaises(WrongCurrentVersion):
            self.client.append_events(
                stream_name,
                current_version=0,
                events=[event2, event3, event4],
            )

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

    def test_resolve_links_when_reading_from_dollar_et_projection(self) -> None:
        if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")

        self.construct_esdb_client()
        event_type = "EventType" + str(uuid4()).replace("-", "")[:5]

        # Append new events (stream does not exist).
        stream_name = str(uuid4())
        # NB only events with JSON data are projected into "$et-{event_type}" streams.
        event1 = NewEvent(type=event_type, data=b"{}")
        event2 = NewEvent(type=event_type, data=b"{}")
        self.client.append_events(
            stream_name, current_version=StreamState.ANY, events=[event1, event2]
        )

        sleep(1)  # Give the projection time to run.

        filtered_events = list(self.client.read_all(filter_include=(event_type,)))
        self.assertGreaterEqual(len(filtered_events), 1)

        projected_events = self.client.get_stream(
            stream_name=f"$et-{event_type}", resolve_links=True
        )

        self.assertEqual(len(filtered_events), len(projected_events))
        for i, event in enumerate(projected_events):
            self.assertEqual(event.id, filtered_events[i].id)

        subscription = self.client.subscribe_to_stream(
            stream_name=f"$et-{event_type}", resolve_links=True
        )
        for i, event in enumerate(subscription):
            self.assertEqual(event.id, filtered_events[i].id)
            if i + 1 == len(filtered_events):
                subscription.stop()

    def test_stream_append_event_with_stream_state_any(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Append new event (works, stream does not exist).
        event1 = NewEvent(type="Snapshot", data=random_data())
        commit_position1 = self.client.append_event(
            stream_name, current_version=StreamState.ANY, event=event1
        )

        # Append new event (works, stream does exist).
        event2 = NewEvent(type="Snapshot", data=random_data())
        commit_position2 = self.client.append_event(
            stream_name, current_version=StreamState.ANY, event=event2
        )

        self.assertGreater(commit_position2, commit_position1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

    def test_stream_append_event_with_stream_state_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        event1 = NewEvent(type="Snapshot", data=random_data())

        # Append new event (fails, stream does not exist).
        with self.assertRaises(WrongCurrentVersion):
            self.client.append_event(
                stream_name, current_version=StreamState.EXISTS, event=event1
            )

        # Append an event so stream exists.
        commit_position1 = self.client.append_event(
            stream_name, current_version=StreamState.NO_STREAM, event=event1
        )

        # Append new event (works, stream exists now).
        event2 = NewEvent(type="Snapshot", data=random_data())
        commit_position2 = self.client.append_event(
            stream_name, current_version=StreamState.EXISTS, event=event2
        )

        self.assertGreater(commit_position2, commit_position1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

    # def test_append_events_multiplexed_without_occ(self) -> None:
    #     self.construct_esdb_client()
    #     stream_name = str(uuid4())
    #
    #     commit_position0 = self.client.get_commit_position()
    #
    #     event1 = NewEvent(type="OrderCreated", data=random_data())
    #     event2 = NewEvent(type="OrderUpdated", data=random_data())
    #
    #     # Append batch of new events.
    #     commit_position2 = self.client.append_events_multiplexed(
    #         stream_name, current_version=StreamState.Any, events=[event1, event2]
    #     )
    #
    #     # Read stream and check recorded events.
    #     events = self.client.get_stream(stream_name)
    #     self.assertEqual(len(events), 2)
    #     self.assertEqual(events[0].id, event1.id)
    #     self.assertEqual(events[1].id, event2.id)
    #
    #     assert commit_position2 > commit_position0
    #     assert commit_position2 == self.client.get_commit_position()
    #     if events[1].commit_position is not None:
    #         assert isinstance(events[0].commit_position, int)
    #         assert events[0].commit_position > commit_position0
    #         assert events[0].commit_position < commit_position2
    #         assert events[1].commit_position == commit_position2
    #
    #     # Append another batch of new events.
    #     event3 = NewEvent(type="OrderUpdated", data=random_data())
    #     event4 = NewEvent(type="OrderUpdated", data=random_data())
    #     commit_position4 = self.client.append_events_multiplexed(
    #         stream_name, current_version=StreamState.Any, events=[event3, event4]
    #     )
    #
    #     # Read stream and check recorded events.
    #     events = self.client.get_stream(stream_name)
    #     self.assertEqual(len(events), 4)
    #     self.assertEqual(events[0].id, event1.id)
    #     self.assertEqual(events[1].id, event2.id)
    #     self.assertEqual(events[2].id, event3.id)
    #     self.assertEqual(events[3].id, event4.id)
    #
    #     assert commit_position4 > commit_position2
    #     assert commit_position4 == self.client.get_commit_position()
    #
    #     if events[3].commit_position is not None:
    #         assert isinstance(events[2].commit_position, int)
    #         assert events[2].commit_position > commit_position2
    #         assert events[2].commit_position < commit_position4
    #         assert events[3].commit_position == commit_position4
    #
    # def test_append_events_multiplexed_with_occ(self) -> None:
    #     self.construct_esdb_client()
    #     stream_name = str(uuid4())
    #
    #     commit_position0 = self.client.get_commit_position()
    #
    #     event1 = NewEvent(type="OrderCreated", data=random_data())
    #     event2 = NewEvent(type="OrderUpdated", data=random_data())
    #
    #     # Fail to append (stream does not exist).
    #     with self.assertRaises(NotFound):
    #         self.client.append_events_multiplexed(
    #             stream_name, current_version=1, events=[event1, event2]
    #         )
    #
    #     # Append batch of new events.
    #     commit_position2 = self.client.append_events_multiplexed(
    #         stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
    #     )
    #
    #     # Read stream and check recorded events.
    #     events = self.client.get_stream(stream_name)
    #     self.assertEqual(len(events), 2)
    #     self.assertEqual(events[0].id, event1.id)
    #     self.assertEqual(events[1].id, event2.id)
    #
    #     assert commit_position2 > commit_position0
    #     assert commit_position2 == self.client.get_commit_position()
    #     if events[1].commit_position is not None:
    #         assert isinstance(events[0].commit_position, int)
    #         assert events[0].commit_position > commit_position0
    #         assert events[0].commit_position < commit_position2
    #         assert events[1].commit_position == commit_position2
    #
    #     # Fail to append (stream already exists).
    #     event3 = NewEvent(type="OrderUpdated", data=random_data())
    #     event4 = NewEvent(type="OrderUpdated", data=random_data())
    #     with self.assertRaises(ExpectedPositionError):
    #         self.client.append_events_multiplexed(
    #             stream_name, current_version=StreamState.NO_STREAM, events=[event3, event4]
    #         )
    #
    #     # Fail to append (wrong expected position).
    #     with self.assertRaises(ExpectedPositionError):
    #         self.client.append_events_multiplexed(
    #             stream_name, current_version=10, events=[event3, event4]
    #         )
    #
    #     # Read stream and check recorded events.
    #     events = self.client.get_stream(stream_name)
    #     self.assertEqual(len(events), 2)
    #     self.assertEqual(events[0].id, event1.id)
    #     self.assertEqual(events[1].id, event2.id)

    def test_stream_append_events_with_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Fail to append (stream does not exist).
        with self.assertRaises(NotFound):
            self.client.append_events(
                stream_name, current_version=1, events=[event1, event2]
            )

        # Append batch of new events.
        commit_position2 = self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
        )

        # Read stream and check recorded events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

        assert commit_position2 > commit_position0
        assert commit_position2 == self.client.get_commit_position()
        if events[1].commit_position is not None:
            assert isinstance(events[0].commit_position, int)
            assert events[0].commit_position > commit_position0
            assert events[0].commit_position < commit_position2
            assert events[1].commit_position == commit_position2

        # Fail to append (stream already exists).
        event3 = NewEvent(type="OrderUpdated", data=random_data())
        event4 = NewEvent(type="OrderUpdated", data=random_data())
        with self.assertRaises(WrongCurrentVersion):
            self.client.append_events(
                stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event3, event4],
            )

        # Fail to append (wrong expected position).
        with self.assertRaises(WrongCurrentVersion):
            self.client.append_events(
                stream_name, current_version=10, events=[event3, event4]
            )

        # Read stream and check recorded events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

    def test_stream_append_events_with_stream_state_any(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append batch of new events.
        commit_position2 = self.client.append_events(
            stream_name, current_version=StreamState.ANY, events=[event1, event2]
        )

        # Read stream and check recorded events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

        assert commit_position2 > commit_position0
        assert commit_position2 == self.client.get_commit_position()
        if events[1].commit_position is not None:
            assert isinstance(events[0].commit_position, int)
            assert events[0].commit_position > commit_position0
            assert events[0].commit_position < commit_position2
            assert events[1].commit_position == commit_position2

        # Append another batch of new events.
        event3 = NewEvent(type="OrderUpdated", data=random_data())
        event4 = NewEvent(type="OrderUpdated", data=random_data())
        commit_position4 = self.client.append_events(
            stream_name, current_version=StreamState.ANY, events=[event3, event4]
        )

        # Read stream and check recorded events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event4.id)

        assert commit_position4 > commit_position2
        assert commit_position4 == self.client.get_commit_position()

        if events[3].commit_position is not None:
            assert isinstance(events[2].commit_position, int)
            assert events[2].commit_position > commit_position2
            assert events[2].commit_position < commit_position4
            assert events[3].commit_position == commit_position4

    def test_stream_append_events_with_stream_state_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append batch of new events.
        with self.assertRaises(NotFound):
            self.client.append_events(
                stream_name, current_version=StreamState.EXISTS, events=[event1, event2]
            )

        commit_position1 = self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
        )

        # Read stream and check recorded events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

        assert commit_position1 > commit_position0
        assert commit_position1 == self.client.get_commit_position()
        if events[1].commit_position is not None:
            assert isinstance(events[0].commit_position, int)
            assert events[0].commit_position > commit_position0
            assert events[0].commit_position < commit_position1
            assert events[1].commit_position == commit_position1

        # Append another batch of new events.
        event3 = NewEvent(type="OrderUpdated", data=random_data())
        event4 = NewEvent(type="OrderUpdated", data=random_data())
        commit_position4 = self.client.append_events(
            stream_name, current_version=StreamState.EXISTS, events=[event3, event4]
        )

        # Read stream and check recorded events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event4.id)

        assert commit_position4 > commit_position1
        assert commit_position4 == self.client.get_commit_position()

        if events[3].commit_position is not None:
            assert isinstance(events[2].commit_position, int)
            assert events[2].commit_position > commit_position1
            assert events[2].commit_position < commit_position4
            assert events[3].commit_position == commit_position4

    def test_commit_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        event1 = NewEvent(type="Snapshot", data=b"{}", metadata=b"{}")

        # Append new event.
        commit_position = self.client.append_events(
            stream_name, current_version=StreamState.ANY, events=[event1]
        )
        # Check we actually have an int.
        self.assertIsInstance(commit_position, int)

        # Check commit_position() returns expected value.
        self.assertEqual(self.client.get_commit_position(), commit_position)

        # Create persistent subscription.
        self.client.create_subscription_to_all(f"group-{uuid4()}")

        # Check commit_position() still returns expected value.
        self.assertEqual(self.client.get_commit_position(), commit_position)

    def test_stream_append_events_raises_deadline_exceeded(self) -> None:
        self.construct_esdb_client()

        large_data = b"a" * 10000
        # Append two events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(
            type="SomethingHappened",
            data=large_data,
        )
        new_events = [event1] * 10000
        # Timeout appending new event.
        with self.assertRaises(GrpcDeadlineExceeded):
            self.client.append_events(
                stream_name=stream_name1,
                current_version=StreamState.NO_STREAM,
                events=new_events,
                timeout=0,
            )

        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name1)

        # # Timeout appending new event.
        # with self.assertRaises(DeadlineExceeded):
        #     self.client.append_events(
        #         stream_name1, current_version=1, events=[event3], timeout=0
        #     )
        #
        # # Timeout reading stream.
        # with self.assertRaises(DeadlineExceeded):
        #     self.client.get_stream(stream_name1, timeout=0)

    def test_read_all_filter_default(self) -> None:
        self.construct_esdb_client()

        num_old_events = len(list(self.client.read_all()))

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        event4 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event5 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event6 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position1 = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        commit_position2 = self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Check we can read forwards from the start.
        events = list(self.client.read_all())
        self.assertEqual(len(events) - num_old_events, 6)
        self.assertEqual(events[-1].stream_name, stream_name2)
        self.assertEqual(events[-1].type, "OrderDeleted")
        self.assertEqual(events[-2].stream_name, stream_name2)
        self.assertEqual(events[-2].type, "OrderUpdated")
        self.assertEqual(events[-3].stream_name, stream_name2)
        self.assertEqual(events[-3].type, "OrderCreated")
        self.assertEqual(events[-4].stream_name, stream_name1)
        self.assertEqual(events[-4].type, "OrderDeleted")

        # Check we can read backwards from the end.
        events = list(self.client.read_all(backwards=True))
        self.assertEqual(len(events) - num_old_events, 6)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderCreated")
        self.assertEqual(events[3].stream_name, stream_name1)
        self.assertEqual(events[3].type, "OrderDeleted")

        # Check we can read forwards from commit position 1.
        events = list(self.client.read_all(commit_position=commit_position1))
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event4.id)
        self.assertEqual(events[2].id, event5.id)
        self.assertEqual(events[3].id, event6.id)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderUpdated")
        self.assertEqual(events[3].stream_name, stream_name2)
        self.assertEqual(events[3].type, "OrderDeleted")

        # Check we can read forwards from commit position 2.
        events = list(self.client.read_all(commit_position=commit_position2))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event6.id)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")

        # Check we can read backwards from commit position 1.
        # NB backwards here doesn't include event at commit position, otherwise
        # first event would an OrderDeleted event, and we get an OrderUpdated.
        events = list(
            self.client.read_all(commit_position=commit_position1, backwards=True)
        )
        self.assertEqual(len(events) - num_old_events, 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event1.id)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name1)
        self.assertEqual(events[1].type, "OrderCreated")

        # Check we can read backwards from commit position 2.
        # NB backwards here doesn't include event at commit position.
        events = list(
            self.client.read_all(commit_position=commit_position2, backwards=True)
        )
        self.assertEqual(len(events) - num_old_events, 5)
        self.assertEqual(events[0].id, event5.id)
        self.assertEqual(events[1].id, event4.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event1.id)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name1)
        self.assertEqual(events[2].type, "OrderDeleted")

        # Check we can read forwards from the start with limit.
        events = list(self.client.read_all(limit=3))
        self.assertEqual(len(events), 3)

        # Check we can read backwards from the end with limit.
        events = list(self.client.read_all(backwards=True, limit=3))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderCreated")

        # Check we can read forwards from commit position 1 with limit.
        events = list(self.client.read_all(commit_position=commit_position1, limit=3))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderUpdated")

        # Check we can read backwards from commit position 2 with limit.
        events = list(
            self.client.read_all(
                commit_position=commit_position2, backwards=True, limit=3
            )
        )
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name1)
        self.assertEqual(events[2].type, "OrderDeleted")

    def assertFilteredEvents(
        self,
        commit_position: int,
        expected: Set[str],
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
    ) -> None:
        events: List[RecordedEvent] = list(
            self.client.read_all(
                commit_position=commit_position,
                filter_exclude=filter_exclude,
                filter_include=filter_include,
                filter_by_stream_name=filter_by_stream_name,
            )
        )
        if filter_by_stream_name is False:
            actual = set([e.type for e in events])
        else:
            actual = set([e.stream_name for e in events])
        self.assertEqual(expected, actual)

    def test_read_all_filter_include_event_types(self) -> None:
        self.construct_esdb_client()

        commit_position = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        event4 = NewEvent(type="InvoiceCreated", data=b"{}", metadata=b"{}")
        event5 = NewEvent(type="InvoiceUpdated", data=b"{}", metadata=b"{}")
        event6 = NewEvent(type="InvoiceDeleted", data=b"{}", metadata=b"{}")
        event7 = NewEvent(type="SomethingElse", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )
        self.client.append_events(
            stream_name1,
            current_version=StreamState.EXISTS,
            events=[event2, event3, event4, event5, event6, event7],
        )

        # Read only OrderCreated.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include="OrderCreated",
            expected={"OrderCreated"},
        )

        # Read only OrderCreated and OrderDeleted.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include=["OrderCreated", "OrderDeleted"],
            expected={"OrderCreated", "OrderDeleted"},
        )

        # Read only Order.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include="Order",
            expected=set(),
        )

        # Read only Updated.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include="Updated",
            expected=set(),
        )

        # Read only Order.*.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include="Order.*",
            expected={"OrderCreated", "OrderUpdated", "OrderDeleted"},
        )

        # Read only Invoice.*.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include="Invoice.*",
            expected={"InvoiceCreated", "InvoiceUpdated", "InvoiceDeleted"},
        )

        # Read only .*Created.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include=".*Created",
            expected={"OrderCreated", "InvoiceCreated"},
        )

        # Read only .*Updated.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include=".*Updated",
            expected={"OrderUpdated", "InvoiceUpdated"},
        )

    def test_read_all_filter_exclude_event_types(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        event4 = NewEvent(type="InvoiceCreated", data=b"{}", metadata=b"{}")
        event5 = NewEvent(type="InvoiceUpdated", data=b"{}", metadata=b"{}")
        event6 = NewEvent(type="InvoiceDeleted", data=b"{}", metadata=b"{}")
        event7 = NewEvent(type="SomethingElse", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )
        self.client.append_events(
            stream_name1,
            current_version=StreamState.EXISTS,
            events=[event2, event3, event4, event5, event6, event7],
        )

        # Exclude OrderCreated. Should exclude event1.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, "OrderCreated"],
            expected={
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude OrderCreated and OrderDeleted. Should exclude event1 and event3.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, "OrderCreated", "OrderDeleted"],
            expected={
                "OrderUpdated",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude Order. Should exclude nothing.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, "Order"],
            expected={
                "OrderCreated",
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude Created. Should exclude nothing.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, "Created"],
            expected={
                "OrderCreated",
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude Order.*. Should exclude event1, event2, event3.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, "Order.*"],
            expected={
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude *.Created. Should exclude event1 and event4.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, r".*Created"],
            expected={
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude *.thing.*. Should exclude event7.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, r".*thing.*"],
            expected={
                "OrderCreated",
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
            },
        )

        # Exclude OrderCreated.+. Should exclude nothing.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, r".OrderCreated.+"],
            expected={
                "OrderCreated",
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

    def test_read_all_filter_include_stream_identifiers(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        prefix1 = str(uuid4())
        prefix2 = str(uuid4())
        stream_name1 = prefix1 + str(uuid4())
        stream_name2 = prefix1 + str(uuid4())
        stream_name3 = prefix2 + str(uuid4())
        commit_position = self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(
            stream_name2, current_version=StreamState.NO_STREAM, events=[event2]
        )
        self.client.append_events(
            stream_name3, current_version=StreamState.NO_STREAM, events=[event3]
        )

        # Read only stream1 and stream2.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=[stream_name1, stream_name2],
            expected={stream_name1, stream_name2},
        )

        # Read only stream2 and stream3.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=[stream_name2, stream_name3],
            expected={stream_name2, stream_name3},
        )

        # Read only prefix1.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=prefix1 + ".*",
            expected={stream_name1, stream_name2},
        )

        # Read only prefix2.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=prefix2 + ".*",
            expected={stream_name3},
        )

    def test_read_all_filter_exclude_stream_identifiers(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        prefix1 = str(uuid4())
        prefix2 = str(uuid4())
        stream_name1 = prefix1 + str(uuid4())
        stream_name2 = prefix1 + str(uuid4())
        stream_name3 = prefix2 + str(uuid4())
        commit_position = self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(
            stream_name2, current_version=StreamState.NO_STREAM, events=[event2]
        )
        self.client.append_events(
            stream_name3, current_version=StreamState.NO_STREAM, events=[event3]
        )

        # Read everything except stream1.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, stream_name1],
            expected={stream_name2, stream_name3},
        )

        # Read everything except stream2 and stream3.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, stream_name2, stream_name3],
            expected={stream_name1},
        )

        # Read everything except prefix1.*.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, prefix1 + ".*"],
            expected={stream_name3},
        )

        # Read everything except prefix2.*.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, prefix2 + ".*"],
            expected={stream_name1, stream_name2},
        )

        # Read everything except prefix2.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX, prefix2],
            expected={stream_name1, stream_name2, stream_name3},
        )

    def test_read_all_filter_include_ignores_filter_exclude(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )
        self.client.append_events(
            stream_name1,
            current_version=StreamState.EXISTS,
            events=[event2, event3],
        )

        # Both include and exclude.
        self.assertFilteredEvents(
            commit_position=commit_position,
            filter_include=["OrderCreated"],
            filter_exclude=["OrderCreated"],
            expected={"OrderCreated"},
        )

    def test_read_all_filter_nothing(self) -> None:
        if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")

        self.construct_esdb_client()

        # Read all events.
        read_response = self.client.read_all(filter_exclude=[])

        for event in read_response:
            if event.stream_name.startswith("$"):
                break

    def test_read_all_resolve_links(self) -> None:
        if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")

        self.construct_esdb_client()
        commit_position = self.client.get_commit_position()

        # Append new events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )

        # Delete a stream. Because then we get a response that has a "link" but not
        # an "event" even though we resolve links. In other words, a link that
        # doesn't have an event. There's a code path for this that we need to cover.
        # See BaseReadResponse._convert_read_resp() where a streams_pb2.ReadResp.ReadEvent
        # a "link streams_pb2.ReadResp.ReadEvent.RecordedEvent" but not an
        # "event streams_pb2.ReadResp.ReadEvent.RecordedEvent".
        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event2],
        )
        self.client.delete_stream(stream_name2, current_version=0)

        sleep(1)  # Give the system time to run.

        # Should get only one instance of event1.
        all_events = tuple(
            self.client.read_all(
                commit_position=commit_position,
                resolve_links=False,
                filter_exclude=[],
            )
        )
        counter = Counter([e.id for e in all_events])
        count = counter.get(event1.id)
        assert isinstance(count, int)
        self.assertEqual(count, 1)

        # Should get more than one instance of event1 due to
        # resolving links generated by system projections.
        all_events = tuple(
            self.client.read_all(
                commit_position=commit_position,
                resolve_links=True,
                filter_exclude=[],
            )
        )
        counter = Counter([e.id for e in all_events])
        count = counter.get(event1.id)
        assert isinstance(count, int)
        self.assertGreater(count, 1)

    def test_stream_delete_with_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())
        event4 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(stream_name, current_version=0, events=[event2])

        # Read stream, expect two events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_current_version(stream_name))

        # Can't delete the stream when specifying incorrect expected position.
        with self.assertRaises(WrongCurrentVersion):
            self.client.delete_stream(stream_name, current_version=0)

        # Delete the stream, specifying correct expected position.
        self.client.delete_stream(stream_name, current_version=1)

        # Can't call delete again with incorrect expected position.
        with self.assertRaises(WrongCurrentVersion):
            self.client.delete_stream(stream_name, current_version=0)

        # Can call delete again, with correct expected position.
        self.client.delete_stream(stream_name, current_version=1)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Expect stream position is None.
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can append to a deleted stream.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event3]
        )
        # with self.assertRaises(ExpectedPositionError):
        #     self.client.append_events(
        #         stream_name,
        #         current_version=StreamState.NO_STREAM,
        #         events=[event3]
        #     )
        # self.client.append_events(stream_name, current_version=1, events=[event3])

        sleep(0.1)  # sometimes we need to wait a little bit for EventStoreDB
        self.assertEqual(2, self.client.get_current_version(stream_name))
        self.client.append_events(stream_name, current_version=2, events=[event4])

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get NotFound.
        sleep(0.1)
        events = self.client.get_stream(stream_name)
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event4.id)

        # Can't delete the stream again with incorrect expected position.
        with self.assertRaises(WrongCurrentVersion):
            self.client.delete_stream(stream_name, current_version=0)

        # Can still read the events.
        self.assertEqual(3, self.client.get_current_version(stream_name))
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Can delete the stream again, using correct expected position.
        self.client.delete_stream(stream_name, current_version=3)

        # Stream is now "not found".
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can't call delete again with incorrect expected position.
        with self.assertRaises(WrongCurrentVersion):
            self.client.delete_stream(stream_name, current_version=2)

        # Can delete again without error.
        self.client.delete_stream(stream_name, current_version=3)

    def test_read_all_raises_deadline_exceeded(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Timeout reading all events.
        read_response = self.client.read_all(timeout=0.001)
        sleep(0.5)
        with self.assertRaises(GrpcDeadlineExceeded):
            list(read_response)

    def test_read_all_can_be_stopped(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Check we do get events when reading all events.
        read_response = self.client.read_all()
        events = list(read_response)
        self.assertNotEqual(0, len(events))

        # Check we don't get events when we stop.
        read_response = self.client.read_all()
        read_response.stop()
        events = list(read_response)
        self.assertEqual(0, len(events))

    def test_stream_delete_with_any_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Can't delete stream that doesn't exist, while expecting "any" version.
        # Todo: I don't fully understand why this should cause an error.
        with self.assertRaises(NotFound):
            self.client.delete_stream(stream_name, current_version=StreamState.ANY)

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(stream_name, current_version=0, events=[event2])

        # Read stream, expect two events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_current_version(stream_name))

        # Delete the stream, specifying "any" expected position.
        self.client.delete_stream(stream_name, current_version=StreamState.ANY)

        # Can call delete again, without error.
        self.client.delete_stream(stream_name, current_version=StreamState.ANY)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Expect stream position is None.
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can append to a deleted stream.
        with self.assertRaises(WrongCurrentVersion):
            self.client.append_events(stream_name, current_version=0, events=[event3])
        self.client.append_events(stream_name, current_version=1, events=[event3])

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get NotFound.
        sleep(0.1)
        events = self.client.get_stream(stream_name)
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event3.id)

        # Delete the stream again, specifying "any" expected position.
        self.client.delete_stream(stream_name, current_version=StreamState.ANY)
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can delete again without error.
        self.client.delete_stream(stream_name, current_version=StreamState.ANY)

    def test_stream_delete_expecting_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Can't delete stream, expecting stream exists, because stream never existed.
        with self.assertRaises(NotFound):
            self.client.delete_stream(
                stream_name, current_version=StreamState.NO_STREAM
            )

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(stream_name, current_version=0, events=[event2])

        # Read stream, expect two events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_current_version(stream_name))

        # Delete the stream, specifying "any" expected position.
        self.client.delete_stream(stream_name, current_version=StreamState.ANY)

        # Can't delete deleted stream, expecting stream exists, because it was deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Expect stream position is None.
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can't append to a deleted stream with incorrect expected position.
        with self.assertRaises(WrongCurrentVersion):
            self.client.append_events(stream_name, current_version=0, events=[event3])

        # Can append to a deleted stream with correct expected position.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event3]
        )

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get NotFound.
        sleep(0.1)
        events = self.client.get_stream(stream_name)
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event3.id)

        # Can delete the appended stream, whilst expecting stream exists.
        self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can't call delete again, expecting stream exists, because it was deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)

    def test_tombstone_stream_with_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(stream_name, current_version=0, events=[event2])

        # Read stream, expect two events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_current_version(stream_name))

        with self.assertRaises(WrongCurrentVersion):
            self.client.tombstone_stream(stream_name, current_version=0)

        # Tombstone the stream, specifying expected position.
        self.client.tombstone_stream(stream_name, current_version=1)

        # Can't tombstone again with correct expected position, stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name, current_version=1)

        # Can't tombstone again with incorrect expected position, stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name, current_version=2)

        # Can't read from stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream(stream_name)

        # Can't get stream position, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.get_current_version(stream_name)

        # Can't append to tombstoned stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.append_events(stream_name, current_version=1, events=[event3])

    def test_tombstone_stream_with_any_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Can tombstone stream that doesn't exist, while expecting "any" version.
        # Todo: I don't really understand why this shouldn't cause an error,
        #  if we can do this with the delete operation.
        self.client.tombstone_stream(stream_name1, current_version=StreamState.ANY)

        # Construct two events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Can't append to tombstoned stream that never existed.
        with self.assertRaises(StreamIsDeleted):
            self.client.append_events(
                stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
            )

        # Append two events to a different stream.
        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(stream_name2, current_version=0, events=[event2])

        # Read stream, expect two events.
        events = self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_current_version(stream_name2))

        # Tombstone the stream, specifying "any" expected position.
        self.client.tombstone_stream(stream_name2, current_version=StreamState.ANY)

        # Can't call tombstone again.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name2, current_version=StreamState.ANY)

        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream(stream_name2)

        with self.assertRaises(StreamIsDeleted):
            self.client.get_current_version(stream_name2)

    def test_tombstone_stream_expecting_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Can't tombstone stream that doesn't exist, while expecting "stream exists".
        with self.assertRaises(NotFound):
            self.client.tombstone_stream(
                stream_name, current_version=StreamState.EXISTS
            )

        # Construct two events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(stream_name, current_version=0, events=[event2])

        # Read stream, expect two events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_current_version(stream_name))

        # Tombstone the stream, expecting "stream exists".
        self.client.tombstone_stream(stream_name, current_version=StreamState.EXISTS)

        # Can't call tombstone again.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(
                stream_name, current_version=StreamState.NO_STREAM
            )

        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream(stream_name)

        with self.assertRaises(StreamIsDeleted):
            self.client.get_current_version(stream_name)

    def test_subscribe_to_all_filter_exclude_system_events(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe to all events, from the start.
        subscription = self.client.subscribe_to_all()
        events = []
        for event in subscription:
            events.append(event)
            if event.id == event3.id:
                break

        # Append three more events.
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Continue reading from the subscription.
        events = []
        for event in subscription:
            events.append(event)
            if event.id == event6.id:
                break

        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event4.id)
        self.assertEqual(events[1].id, event5.id)
        self.assertEqual(events[2].id, event6.id)

    def test_subscribe_to_all_filter_exclude_nothing(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe and exclude nothing.
        subscription = self.client.subscribe_to_all(
            filter_exclude=[],
        )

        # Expect to get system events.
        for event in subscription:
            if event.type.startswith("$"):
                break
            else:
                self.fail("Didn't get the $metadata event")

    def test_subscribe_to_all_filter_include_event_types(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe from the beginning.
        subscription = self.client.subscribe_to_all(
            filter_include=["OrderCreated"],
        )

        # Expect to only get "OrderCreated" events.
        events = []
        for event in subscription:
            if not event.type.startswith("OrderCreated"):
                self.fail("Event type is not 'OrderCreated'")

            events.append(event)

            # Break if we see the 'OrderCreated' event appended above.
            if event.id == event1.id:
                break

        # Check we actually got some 'OrderCreated' events.
        self.assertGreater(len(events), 0)

    def test_subscribe_to_all_filter_include_stream_names(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        stream_name2 = str(uuid4())
        stream_name3 = str(uuid4())
        self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(
            stream_name2, current_version=StreamState.NO_STREAM, events=[event2]
        )
        self.client.append_events(
            stream_name3, current_version=StreamState.NO_STREAM, events=[event3]
        )

        # Subscribe to all, filtering by stream name for stream_name1.
        subscription = self.client.subscribe_to_all(
            filter_include=stream_name1, filter_by_stream_name=True
        )

        # Expect to only get stream_name1 events.
        for event in subscription:
            if event.stream_name != stream_name1:
                self.fail("Filtering included other stream names")
            if event.id == event1.id:
                break

        # Subscribe to all, filtering by stream name for stream_name2.
        subscription = self.client.subscribe_to_all(
            filter_include=stream_name2, filter_by_stream_name=True
        )

        # Expect to only get stream_name2 events.
        for event in subscription:
            if event.stream_name != stream_name2:
                self.fail("Filtering included other stream names")
            if event.id == event2.id:
                break

        # Subscribe to all, filtering by stream name for stream_name3.
        subscription = self.client.subscribe_to_all(
            filter_include=stream_name3, filter_by_stream_name=True
        )

        # Expect to only get stream_name3 events.
        for event in subscription:
            if event.stream_name != stream_name3:
                self.fail("Filtering included other stream names")
            if event.id == event3.id:
                break

    def test_subscribe_to_all_include_checkpoints(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe excluding all events, with small window.
        subscription = self.client.subscribe_to_all(
            filter_exclude=".*",
            include_checkpoints=True,
            window_size=1,
            checkpoint_interval_multiplier=1,
        )

        # Expect to get checkpoints.
        for event in subscription:
            if isinstance(event, Checkpoint):
                break
        else:
            self.fail("Didn't get a checkpoint")

    @skipIf("21.10" in EVENTSTORE_IMAGE_TAG, "'Extra checkpoint' bug")
    @skipIf("22.10" in EVENTSTORE_IMAGE_TAG, "'Extra checkpoint' bug")
    def test_extra_checkpoint_bug_is_fixed(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        first_append_commit_position = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        def get_event_at_commit_position(commit_position: int) -> RecordedEvent:
            read_response = self.client.read_all(
                commit_position=commit_position,
                # backwards=True,
                filter_exclude=[],
                limit=1,
            )
            events = tuple(read_response)
            assert len(events) == 1, len(events)
            event = events[0]
            assert event.commit_position == commit_position, event
            return event

        event = get_event_at_commit_position(first_append_commit_position)
        self.assertEqual(event.id, event3.id)
        self.assertEqual(event.commit_position, first_append_commit_position)
        current_commit_position = self.client.get_commit_position(filter_exclude=[])
        self.assertEqual(event.commit_position, current_commit_position)

        # Subscribe excluding all events, with large window.
        subscription1 = self.client.subscribe_to_all(
            # filter_exclude=[".*"],
            include_checkpoints=True,
            window_size=10000,
            checkpoint_interval_multiplier=500,
            timeout=5,
        )

        # We shouldn't get an extra checkpoint at the end (ESDB bug < v23.10).
        with self.assertRaises(GrpcDeadlineExceeded):
            for event in subscription1:
                if isinstance(event, Checkpoint):
                    break
            self.fail("Server has 'extra checkpoint' bug. Please use v23.10 or later.")

    #
    # Disabled this test, bc the 'extra checkpoint' bug was fixed in LTS version 23.10.
    #
    # def test_demonstrate_checkpoint_has_unused_commit_position(self) -> None:
    #
    #     self.construct_esdb_client()
    #
    #     # Append new events.
    #     event1 = NewEvent(type="OrderCreated", data=random_data())
    #     event2 = NewEvent(type="OrderUpdated", data=random_data())
    #     event3 = NewEvent(type="OrderDeleted", data=random_data())
    #     stream_name1 = str(uuid4())
    #     first_append_commit_position = self.client.append_events(
    #         stream_name1,
    #         current_version=StreamState.NO_STREAM,
    #         events=[event1, event2, event3],
    #     )
    #
    #     def get_event_at_commit_position(commit_position: int) -> RecordedEvent:
    #         read_response = self.client.read_all(
    #             commit_position=commit_position,
    #             # backwards=True,
    #             filter_exclude=[],
    #             limit=1,
    #         )
    #         events = tuple(read_response)
    #         assert len(events) == 1, len(events)
    #         event = events[0]
    #         assert event.commit_position == commit_position, event
    #         return event
    #
    #     event = get_event_at_commit_position(first_append_commit_position)
    #     self.assertEqual(event.id, event3.id)
    #     self.assertEqual(event.commit_position, first_append_commit_position)
    #     current_commit_position = self.client.get_commit_position(filter_exclude=[])
    #     self.assertEqual(event.commit_position, current_commit_position)
    #
    #     # Subscribe excluding all events, with large window.
    #     subscription1 = self.client.subscribe_to_all(
    #         # filter_exclude=[".*"],
    #         include_checkpoints=True,
    #         window_size=10000,
    #         checkpoint_interval_multiplier=500,
    #         timeout=10
    #     )
    #
    #     # We always get a checkpoint at the end..... why?
    #     try:
    #         for event in subscription1:
    #             if isinstance(event, Checkpoint):
    #                 last_checkpoint_commit_position = event.commit_position
    #                 break
    #             else:
    #                 pass
    #     except DeadlineExceeded:
    #         self.fail("We didn't get the extra checkpoint! Hooray!")
    #
    #     # Sadly, the checkpoint commit position doesn't correspond
    #     # to an event that has been filtered out.
    #     with self.assertRaises(AssertionError):
    #         assert event.commit_position is not None
    #         get_event_at_commit_position(event.commit_position)
    #
    #     # And the checkpoint commit position is greater than the current commit position.
    #     assert last_checkpoint_commit_position is not None
    #     self.assertLess(
    #         self.client.get_commit_position(filter_exclude=[]),
    #         last_checkpoint_commit_position,
    #     )
    #
    #     # And the checkpoint commit position is allocated to the next appended new event.
    #     event4 = NewEvent(type="OrderCreated", data=random_data())
    #     stream_name2 = str(uuid4())
    #     next_append_commit_position = self.client.append_events(
    #         stream_name2,
    #         current_version=StreamState.NO_STREAM,
    #         events=[event4],
    #     )
    #     self.assertEqual(next_append_commit_position, last_checkpoint_commit_position)
    #
    #     # Which means that if a downstream event-processing component is going to
    #     # restart a catch-up subscription from last_checkpoint_commit_position,
    #     # it would not receive event4.
    #
    #     event5 = NewEvent(type="OrderCreated", data=random_data())
    #     stream_name3 = str(uuid4())
    #     self.client.append_events(
    #         stream_name3,
    #         current_version=StreamState.NO_STREAM,
    #         events=[event5],
    #     )
    #
    #     subscription2 = self.client.subscribe_to_all(
    #         commit_position=last_checkpoint_commit_position
    #     )
    #     next_event_from_2 = next(subscription2)
    #     assert isinstance(next_event_from_2.commit_position, int)
    #     self.assertGreater(
    #         next_event_from_2.commit_position, last_checkpoint_commit_position
    #     )
    #     self.assertNotEqual(next_event_from_2.id, event4.id)
    #     self.assertEqual(next_event_from_2.id, event5.id)
    #
    #     next_event_from_1 = next(subscription1)
    #     self.assertEqual(next_event_from_1.id, event4.id)
    #     self.assertEqual(
    #         next_event_from_1.commit_position, last_checkpoint_commit_position
    #     )

    def test_subscribe_to_all_from_commit_position_zero(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe from the beginning.
        subscription = self.client.subscribe_to_all()

        # Expect to only get "OrderCreated" events.
        count = 0
        for _ in subscription:
            count += 1
            break
        self.assertEqual(count, 1)

    def test_subscribe_to_all_from_commit_position_current(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        commit_position = self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(
            stream_name1, current_version=0, events=[event2, event3]
        )

        # Subscribe from the commit position.
        subscription = self.client.subscribe_to_all(commit_position=commit_position)

        events = []
        for event in subscription:
            # Expect catch-up subscription results are exclusive of given
            # commit position, so that we expect event1 to be not included.
            if event.id == event1.id:
                self.fail("Not exclusive")

            # Collect events.
            events.append(event)

            # Break if we got the last one we wrote.
            if event.id == event3.id:
                break

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event3.id)

    def test_subscribe_to_all_from_end(self) -> None:
        self.construct_esdb_client()

        # Append an event.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )

        # Subscribe from end.
        subscription = self.client.subscribe_to_all(from_end=True)

        # Append more events.
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=0, events=[event2, event3]
        )

        events = []
        for event in subscription:
            # Expect catch-up subscription results are exclusive of given
            # commit position, so that we expect event1 to be not included.
            if event.id == event1.id:
                self.fail("Not from end")

            # Collect events.
            events.append(event)

            # Break if we got the last one we wrote.
            if event.id == event3.id:
                break

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event3.id)

    def test_subscribe_to_all_raises_deadline_exceeded(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        retries = 30  # retries because sometimes this times out too fast
        while retries:
            retries -= 1
            # Subscribe from the beginning.
            subscription = self.client.subscribe_to_all(timeout=0.25)

            # Expect to timeout instead of waiting indefinitely for next event.
            count = 0
            with self.assertRaises(GrpcDeadlineExceeded):
                for _ in subscription:
                    count += 1
            if count > 0:
                break
        else:
            self.fail("Didn't get any events before deadline, despite retries")

    def test_subscribe_to_all_can_be_stopped(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe from the beginning.
        subscription = self.client.subscribe_to_all()

        # Stop subscription.
        subscription.stop()

        # Iterating should stop.
        list(subscription)

    def _test_subscribe_to_all_raises_consumer_too_slow(self) -> None:
        # Todo: The server behaviour is too unreliable to run this test.
        self.construct_esdb_client()

        # Subscribe from the end.
        subscription = self.client.subscribe_to_all(
            commit_position=self.client.get_commit_position()
        )

        # Append new events.
        with self.assertRaises(ConsumerTooSlow):
            while True:
                # Write 10000 events.
                commit_position = self.client.append_events(
                    stream_name=str(uuid4()),
                    current_version=StreamState.NO_STREAM,
                    events=[
                        NewEvent(type=f"Type{i}", data=b"{}") for i in range(10000)
                    ],
                )
                print(commit_position)
                sleep(0.1)
                # Read one event.
                next(subscription)

    def _a_better_test_subscribe_to_all_raises_consumer_too_slow(self) -> None:
        # Todo: The server behaviour is too unreliable to run this test.

        self.construct_esdb_client()

        # Subscribe from the beginning.
        subscription = self.client.subscribe_to_all()

        # Append new events.
        for i in range(1000):
            # Write 100 events.
            self.client.append_events(
                stream_name=str(uuid4()),
                current_version=StreamState.NO_STREAM,
                events=[NewEvent(type=f"Type{i}", data=b"{}") for i in range(1000)],
            )
            # Read one event.
            try:
                next(subscription)
            except ConsumerTooSlow:
                break
        else:
            self.fail("Didn't see 'ConsumerTooSlow' error")

    def test_subscribe_to_stream_from_start(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe to stream events, from the start.
        subscription = self.client.subscribe_to_stream(stream_name=stream_name1)
        events = []
        for event in subscription:
            events.append(event)
            if event.id == event3.id:
                break

        # Append three events to stream2.
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Append three more events to stream1.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=2, events=[event7, event8, event9]
        )

        # Continue reading from the subscription.
        events = []
        for event in subscription:
            events.append(event)
            if event.id == event9.id:
                break

        # Check we got events only from stream1.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event7.id)
        self.assertEqual(events[1].id, event8.id)
        self.assertEqual(events[2].id, event9.id)

    def test_subscribe_to_stream_from_end(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe to stream events, from the end.
        subscription = self.client.subscribe_to_stream(
            stream_name=stream_name1, from_end=True
        )

        # Append three events to stream2.
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Append three more events to stream1.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=2, events=[event7, event8, event9]
        )

        # Continue reading from the subscription.
        events = []
        for event in subscription:
            events.append(event)
            if event.id == event9.id:
                break

        # Check we got events only from stream1 after we subscribed.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event7.id)
        self.assertEqual(events[1].id, event8.id)
        self.assertEqual(events[2].id, event9.id)

    def test_subscribe_to_stream_from_stream_position(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Subscribe to stream events, from the current stream position.
        subscription = self.client.subscribe_to_stream(
            stream_name=stream_name1, stream_position=1
        )
        events = []
        for event in subscription:
            events.append(event)
            if event.id == event3.id:
                break

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event3.id)

        # Append three events to stream2.
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Append three more events to stream1.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=2, events=[event7, event8, event9]
        )

        # Continue reading from the subscription.
        for event in subscription:
            events.append(event)
            if event.id == event9.id:
                break

        # Check we got events only from stream1.
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event7.id)
        self.assertEqual(events[2].id, event8.id)
        self.assertEqual(events[3].id, event9.id)

    def test_subscribe_to_stream_can_be_stopped(self) -> None:
        self.construct_esdb_client()

        # Subscribe to a stream.
        stream_name1 = str(uuid4())
        subscription = self.client.subscribe_to_stream(stream_name=stream_name1)

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Stop subscription.
        subscription.stop()

        # Iterating should stop.
        list(subscription)

    def test_subscription_to_all_read_with_ack_event_id(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.ack(event.ack_id)
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_read_with_ack_event_object(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_read_with_nack_unknown(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.nack(event, action="unknown")
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_read_with_nack_park(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.nack(event, action="park")
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_replay_parked(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(
            group_name=group_name, timeout=30
        )

        # Park event.
        parked_events = []
        for event in subscription:
            subscription.nack(event, action="park")
            parked_events.append(event)
            if event.id == event3.id:
                break

        # Sleep before calling replay_parked_events() so EventStoreDB catches up.
        sleep(0.5)
        self.client.replay_parked_events(group_name=group_name)

        replayed_events = []
        for event in subscription:
            subscription.ack(event)
            replayed_events.append(event)
            if event.id == event3.id:
                subscription.stop()

        assert replayed_events[-3].data == event1.data
        assert replayed_events[-2].data == event2.data
        assert replayed_events[-1].data == event3.data

    def test_subscription_to_all_ack_with_wrong_object(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(
            group_name=group_name, timeout=30
        )

        class NotUUID:
            pass

        # Ack with wrong type of object.
        with self.assertRaises(ExceptionIteratingRequests) as cm:
            for _ in subscription:
                subscription.ack(NotUUID())  # type: ignore
        self.assertIsInstance(cm.exception.__cause__, AssertionError)

    def test_subscription_to_stream_replay_parked(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        stream_name1 = str(uuid4())
        self.client.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name1
        )

        # Append three events.
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_stream(
            group_name=group_name, stream_name=stream_name1
        )

        # Park event.
        parked_events = []
        for event in subscription:
            subscription.nack(event, action="park")
            parked_events.append(event)
            if event.id == event3.id:
                break

        # Sleep before calling replay_parked_events() so EventStoreDB catches up.
        sleep(0.5)
        self.client.replay_parked_events(
            group_name=group_name, stream_name=stream_name1
        )

        replayed_events = []
        for event in subscription:
            subscription.ack(event)
            replayed_events.append(event)
            if event.id == event3.id:
                subscription.stop()

        assert replayed_events[-3].data == event1.data
        assert replayed_events[-2].data == event2.data
        assert replayed_events[-1].data == event3.data

    def test_subscription_to_all_read_with_nack_retry(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name,
            from_end=True,
        )

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.nack(event, action="retry")
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

        # Should get the events again.
        expected_event_ids = {event1.id, event2.id, event3.id}
        for event in subscription:
            subscription.ack(event)
            if event.id in expected_event_ids:
                expected_event_ids.remove(event.id)
            if len(expected_event_ids) == 0:
                break

    def test_subscription_to_all_read_with_nack_skip(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read all events.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.nack(event, action="skip")
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_read_with_nack_stop(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read all events.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.nack(event, action="stop")
            events.append(event)
            if event.id == event3.id:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_read_with_message_timeout_event_buffer_size_1(
        self,
    ) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name, from_end=True, message_timeout=1
        )

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read all events.
        subscription = self.client.read_subscription_to_all(
            group_name=group_name, event_buffer_size=1, max_ack_batch_size=1
        )

        events = []
        for event in subscription:
            # if event.id == event1.id:
            #     print("Received event1")
            # if event.id == event2.id:
            #     print("Received event2")
            # if event.id == event3.id:
            #     print("Received event3")
            events.append(event)
            if event.id == event1.id and event.retry_count == 0:
                continue
            subscription.ack(event)
            if len(events) == 4:
                break

        assert events[-4].data == event1.data
        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_subscription_to_all_read_with_message_timeout_event_buffer_size_10(
        self,
    ) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name, from_end=True, message_timeout=1
        )

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read all events.
        subscription = self.client.read_subscription_to_all(
            group_name=group_name, event_buffer_size=10, max_ack_batch_size=1
        )

        events = []
        for event in subscription:
            # if event.id == event1.id:
            #     print("Received event1")
            # if event.id == event2.id:
            #     print("Received event2")
            # if event.id == event3.id:
            #     print("Received event3")
            events.append(event)
            if event.id == event1.id and event.retry_count == 0:
                continue
            subscription.ack(event)
            if len(events) == 4:
                break

        assert events[-4].data == event1.data
        assert events[-3].data == event2.data
        assert events[-2].data == event3.data
        assert events[-1].data == event1.data

    def test_subscription_to_all_read_with_max_retry_count_3(
        self,
    ) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name, from_end=True, message_timeout=0.1, max_retry_count=3
        )

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read all events.
        subscription = self.client.read_subscription_to_all(
            group_name=group_name,
            event_buffer_size=1,
        )

        events = []
        for event in subscription:
            events.append(event)
            if event.id == event1.id:
                continue
            subscription.ack(event)
            if len(events) == 6:
                break

        assert events[-6].data == event1.data
        assert events[-5].data == event1.data
        assert events[-4].data == event1.data
        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    # def test_subscription_to_all_read_with_message_timeout_consumer_crashes_and_resumes(
    #     self,
    # ) -> None:
    #     self.construct_esdb_client()
    #
    #     # Create persistent subscription (large message timeout).
    #     group_name = f"my-subscription-{uuid4().hex}"
    #     self.client.create_subscription_to_all(
    #         group_name=group_name,
    #         from_end=True,
    #         message_timeout=10,
    #         consumer_strategy="RoundRobin",
    #         # consumer_strategy="Pinned",
    #     )
    #
    #     # Append three events.
    #     stream_name1 = str(uuid4())
    #
    #     event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
    #     event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
    #     event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
    #     event4 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
    #     event5 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
    #     event6 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
    #
    #     self.client.append_events(
    #         stream_name1,
    #         current_version=StreamState.NO_STREAM,
    #         events=[event1, event2, event3, event4, event5, event6],
    #     )
    #
    #     # Read all events.
    #     subscription1 = self.client.read_subscription_to_all(
    #         group_name=group_name, event_buffer_size=10, max_ack_batch_size=1
    #     )
    #     subscription2 = self.client.read_subscription_to_all(
    #         group_name=group_name, event_buffer_size=10, max_ack_batch_size=1
    #     )
    #
    #     for event in subscription1:
    #         # if event.id == event1.id:
    #         #     print("Subscription1 received event1")
    #         # if event.id == event2.id:
    #         #     print("Subscription1 received event2")
    #         # if event.id == event3.id:
    #         #     print("Subscription1 received event3")
    #         # if event.id == event4.id:
    #         #     print("Subscription1 received event4")
    #         # if event.id == event5.id:
    #         #     print("Subscription1 received event5")
    #         # if event.id == event6.id:
    #         #     print("Subscription1 received event6")
    #         self.assertEqual(event.id, event1.id)
    #         # subscription1.ack(event)
    #         break  # Fail to ack event1 and crash out.
    #
    #     # del subscription1
    #     subscription1.stop()  # If we don't stop(), then subscription2 is severely delayed.
    #
    #     # Read all events.
    #
    #     try:
    #         events = []
    #         for event in subscription2:
    #             # if event.id == event1.id:
    #             #     print("Subscription2 received event1")
    #             # if event.id == event2.id:
    #             #     print("Subscription2 received event2")
    #             # if event.id == event3.id:
    #             #     print("Subscription2 received event3")
    #             # if event.id == event4.id:
    #             #     print("Subscription2 received event4")
    #             # if event.id == event5.id:
    #             #     print("Subscription2 received event5")
    #             # if event.id == event6.id:
    #             #     print("Subscription2 received event6")
    #             events.append(event)
    #             subscription2.ack(event)
    #             if len(events) == 6:
    #                 break
    #
    #         assert events[-3].data == event1.data
    #         assert events[-2].data == event2.data
    #         assert events[-1].data == event3.data
    #     finally:
    #         pass
    #         # subscription2.stop()
    #     # subscription1.stop()

    def test_subscription_to_all_can_be_stopped(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        # Stop subscription.
        subscription.stop()

        # Check we received zero events.
        events = list(subscription)
        assert len(events) == 0

    def test_subscription_to_all_from_commit_position(self) -> None:
        self.construct_esdb_client()

        # Append one event.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        commit_position = self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )

        # Append two more events.
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=0, events=[event2, event3]
        )

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"

        self.client.create_subscription_to_all(
            group_name=group_name,
            commit_position=commit_position,
        )

        # Read events from subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.ack(event)

            events.append(event)

            if event.id == event3.id:
                break

        # Expect persistent subscription results are inclusive of given
        # commit position, so that we expect event1 to be included.

        assert events[0].id == event1.id
        assert events[1].id == event2.id
        assert events[2].id == event3.id

    def test_subscription_to_all_from_end(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name,
            from_end=True,
            # filter_exclude=[],
        )

        # Append three events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read three events.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        events = []
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event3.id:
                break

        # Expect persistent subscription to return only new events appended
        # after subscription was created. Although persistent subscription is
        # inclusive when specifying commit position, and "end" surely refers
        # to a commit position, the event at "end" happens to be the
        # "PersistentConfig" event, and we are filtering this out by default.
        # If this test is adjusted to set filter_exclude=[] the "PersistentConfig"
        # event is returned as the first event from the response.
        assert events[0].data == event1.data
        assert events[1].data == event2.data
        assert events[2].data == event3.data

    def test_subscription_to_all_filter_exclude_event_types(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name,
            filter_exclude=["OrderCreated"],
            from_end=True,
        )

        # Append three events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Read events from subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        # Check we don't receive any OrderCreated events.
        for event in subscription:
            subscription.ack(event)
            self.assertNotEqual(event.type, "OrderCreated")
            if event.id == event3.id:
                break

    def test_subscription_to_all_filter_exclude_stream_names(self) -> None:
        self.construct_esdb_client()

        stream_name1 = str(uuid4())
        prefix1 = str(uuid4())
        stream_name2 = prefix1 + str(uuid4())
        stream_name3 = prefix1 + str(uuid4())
        stream_name4 = str(uuid4())

        # Create persistent subscriptions.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name1,
            filter_exclude=stream_name1,
            filter_by_stream_name=True,
            from_end=True,
        )
        group_name2 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name2,
            filter_exclude=prefix1 + ".*",
            filter_by_stream_name=True,
            from_end=True,
        )

        # Append events.
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
        event4 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(
            stream_name2, current_version=StreamState.NO_STREAM, events=[event2]
        )
        self.client.append_events(
            stream_name3, current_version=StreamState.NO_STREAM, events=[event3]
        )
        self.client.append_events(
            stream_name4, current_version=StreamState.NO_STREAM, events=[event4]
        )

        # Check we don't receive any events from stream_name1.
        subscription = self.client.read_subscription_to_all(group_name=group_name1)

        for event in subscription:
            if event.stream_name == stream_name1:
                self.fail("Received event from stream_name1")
            subscription.ack(event)
            if event.id == event4.id:
                break

        # Check we don't receive any events from stream names starting with prefix1.
        subscription = self.client.read_subscription_to_all(group_name=group_name2)

        for event in subscription:
            if event.stream_name.startswith(prefix1):
                self.fail("Received event with stream name starting with prefix1")
            subscription.ack(event)
            if event.id == event4.id:
                break

    def test_subscription_to_all_filter_include_event_types(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name,
            filter_include=["OrderCreated"],
            from_end=True,
        )

        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Check we only receive any OrderCreated events.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        for event in subscription:
            subscription.ack(event)
            self.assertEqual(event.type, "OrderCreated")
            if event.id == event1.id:
                break

    def test_subscription_to_all_filter_include_stream_names(self) -> None:
        self.construct_esdb_client()

        stream_name1 = str(uuid4())
        prefix1 = str(uuid4())
        stream_name2 = str(uuid4())
        stream_name3 = prefix1 + str(uuid4())
        stream_name4 = prefix1 + str(uuid4())

        # Create persistent subscriptions.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name1,
            filter_include=stream_name4,
            filter_by_stream_name=True,
            from_end=True,
        )
        group_name2 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name2,
            filter_include=prefix1 + ".*",
            filter_by_stream_name=True,
            from_end=True,
        )

        # Append events.
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
        event4 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, current_version=StreamState.NO_STREAM, events=[event1]
        )
        self.client.append_events(
            stream_name2, current_version=StreamState.NO_STREAM, events=[event2]
        )
        self.client.append_events(
            stream_name3, current_version=StreamState.NO_STREAM, events=[event3]
        )
        self.client.append_events(
            stream_name4, current_version=StreamState.NO_STREAM, events=[event4]
        )

        # Check we only receive events from stream4.
        subscription = self.client.read_subscription_to_all(group_name=group_name1)

        events = []
        for event in subscription:
            subscription.ack(event1.id)
            events.append(event)
            if event.id == event4.id:
                break

        self.assertEqual(1, len(events))
        self.assertEqual(events[0].id, event4.id)

        # Check we only receive events with stream name starting with prefix1.
        subscription = self.client.read_subscription_to_all(group_name=group_name2)

        events = []
        for event in subscription:
            events.append(event)
            subscription.ack(event)
            if event.id == event4.id:
                break

        self.assertEqual(2, len(events))
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event4.id)

    def test_subscription_to_all_filter_nothing(self) -> None:
        if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name,
            filter_exclude=[],
        )

        # Read events from subscription.
        subscription = self.client.read_subscription_to_all(group_name=group_name)

        for event in subscription:
            subscription.ack(event)
            # Look for a "system" event.
            if event.type.startswith("$"):
                break

    def test_subscription_to_all_resolve_links(self) -> None:
        if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")
        self.construct_esdb_client()
        commit_position = self.client.get_commit_position()

        event_type = "EventType" + str(uuid4()).replace("-", "")[:5]

        # Append an event.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type=event_type, data=b"{}")
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )

        # Delete a stream. Because then we get a response that has a "link" but not
        # an "event" even though we resolve links. In other words, it's a link that
        # doesn't have an event. There's a code path for this that we need to cover.
        # See BasePersistentSubscription._construct_recorded_event() where a
        # persistent_pb2.ReadResp.ReadEvent has a
        # "link persistent_pb2.ReadResp.ReadEvent.RecordedEvent" but not an
        # "event persistent_pb2.ReadResp.ReadEvent.RecordedEvent".
        stream_name2 = str(uuid4())
        event2 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event2],
        )
        self.client.delete_stream(stream_name2, current_version=0)

        sleep(1)  # Give the system time to run.

        # Create persistent subscription to all.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name,
            commit_position=commit_position,
            resolve_links=True,
            filter_exclude=[],
        )

        # Read subscription to all.
        subscription = self.client.read_subscription_to_all(
            group_name=group_name,
        )

        # Wait for more than one instance of event1 (resolved system generated links).
        counter = 0
        for event in subscription:
            subscription.ack(event)
            if event.id == event1.id:
                counter += 1
            if counter > 1:
                break

    def test_subscription_to_all_with_consumer_strategy_round_robin(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name1, consumer_strategy="RoundRobin", from_end=True
        )

        # Multiple consumers.
        subscription1 = self.client.read_subscription_to_all(group_name=group_name1)
        subscription2 = self.client.read_subscription_to_all(group_name=group_name1)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        # Append three more events.
        stream_name2 = str(uuid4())

        event4 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event5 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event6 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        events1 = []
        events2 = []
        while True:
            event = next(subscription1)
            subscription1.ack(event)
            events1.append(event)
            if event.id == event3.id:
                break
            event = next(subscription2)
            subscription2.ack(event)
            events2.append(event)
            if event.id == event3.id:
                break

        len1 = len(events1)
        self.assertTrue(len1)
        len2 = len(events2)
        self.assertTrue(len2)

        # Check the consumers have received an equal number of events.
        self.assertLess((len1 - len2) ** 2, 2)

    def test_subscription_to_all_max_subscriber_count(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name1, max_subscriber_count=2, from_end=True
        )

        # Multiple consumers.
        subscription1 = self.client.read_subscription_to_all(group_name=group_name1)
        subscription2 = self.client.read_subscription_to_all(group_name=group_name1)
        with self.assertRaises(MaximumSubscriptionsReached):
            self.client.read_subscription_to_all(group_name=group_name1)
        subscription1.stop()
        subscription2.stop()

    def test_subscription_get_info(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(NotFound):
            self.client.get_subscription_info(group_name)
        # Create persistent subscription.
        self.client.create_subscription_to_all(group_name=group_name)

        info = self.client.get_subscription_info(group_name)
        self.assertEqual(info.group_name, group_name)

    def test_subscriptions_list(self) -> None:
        self.construct_esdb_client()

        subscriptions_before = self.client.list_subscriptions()

        # Create subscription to all.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name1,
            filter_exclude=[],
            filter_include=[],
        )

        # Create subscription to stream.
        group_name2 = f"my-subscription-{uuid4().hex}"
        stream_name = str(uuid4())
        self.client.create_subscription_to_stream(
            group_name=group_name2,
            stream_name=stream_name,
        )

        # List subscriptions.
        subscriptions_after = self.client.list_subscriptions()

        # List includes both the "subscription to all" and the "subscription to stream".
        self.assertEqual(len(subscriptions_before) + 2, len(subscriptions_after))
        group_names = [s.group_name for s in subscriptions_after]
        self.assertIn(group_name1, group_names)
        self.assertIn(group_name2, group_names)

    def test_subscription_to_all_already_exists(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        # Create persistent subscription.
        self.client.create_subscription_to_all(group_name)

        # Try to create same persistent subscription.
        with self.assertRaises(AlreadyExists):
            self.client.create_subscription_to_all(group_name)

    def test_subscription_to_all_update(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        # Can't update a subscription that doesn't exist.
        with self.assertRaises(NotFound):
            self.client.update_subscription_to_all(group_name=group_name)

        # Create persistent subscription.
        self.client.create_subscription_to_all(
            group_name=group_name,
            filter_exclude=[],
            filter_include=[],
        )

        # Can update a subscription that does exist.
        self.client.update_subscription_to_all(group_name=group_name)

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")

        # Update subscription to run from end.
        self.client.update_subscription_to_all(group_name=group_name, from_end=True)
        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:-1/P:-1")

        # Update subscription to run from commit position.
        commit_position = self.client.get_commit_position()
        self.client.update_subscription_to_all(
            group_name=group_name, commit_position=commit_position
        )
        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, f"C:{commit_position}/P:{commit_position}")

        # Update subscription to run from start.
        self.client.update_subscription_to_all(group_name=group_name)
        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.start_from, "C:0/P:0")

        # Update subscription to resolve links.
        self.assertFalse(info.resolve_link_tos)
        self.client.update_subscription_to_all(
            group_name=group_name, resolve_links=True
        )
        info = self.client.get_subscription_info(group_name=group_name)
        self.assertTrue(info.resolve_link_tos)

        # Update subscription's max_subscriber_count.
        self.assertEqual(5, info.max_subscriber_count)
        self.client.update_subscription_to_all(
            group_name=group_name, max_subscriber_count=10
        )
        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(10, info.max_subscriber_count)

    def test_subscription_to_all_message_timeout_setting(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"

        # Create persistent subscription.
        self.client.create_subscription_to_all(
            group_name=group_name,
            message_timeout=60,
        )

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.message_timeout_milliseconds, 60000)

        # Update subscription.
        self.client.update_subscription_to_all(
            group_name=group_name,
            message_timeout=1,
        )

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.message_timeout_milliseconds, 1000)

        # Update subscription (check rounding to nearest millisecond).
        self.client.update_subscription_to_all(
            group_name=group_name,
            message_timeout=1.0003,
        )

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.message_timeout_milliseconds, 1000)

        # Update subscription (check rounding to nearest millisecond).
        self.client.update_subscription_to_all(
            group_name=group_name,
            message_timeout=1.0009,
        )

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.message_timeout_milliseconds, 1001)

    def test_subscription_to_all_max_retry_count_setting(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"

        # Create persistent subscription.
        self.client.create_subscription_to_all(
            group_name=group_name,
            max_retry_count=60,
        )

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.max_retry_count, 60)

        # Update subscription.
        self.client.update_subscription_to_all(
            group_name=group_name,
            max_retry_count=1,
        )

        info = self.client.get_subscription_info(group_name=group_name)
        self.assertEqual(info.max_retry_count, 1)

    def test_subscription_delete(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        # Can't delete a subscription that doesn't exist.
        with self.assertRaises(NotFound):
            self.client.delete_subscription(group_name=group_name)

        # Create persistent subscription.
        self.client.create_subscription_to_all(
            group_name=group_name,
            filter_exclude=[],
            filter_include=[],
        )

        subscriptions_before = self.client.list_subscriptions()
        group_names = [s.group_name for s in subscriptions_before]
        self.assertIn(group_name, group_names)

        self.client.delete_subscription(group_name=group_name)

        subscriptions_after = self.client.list_subscriptions()
        self.assertEqual(len(subscriptions_before) - 1, len(subscriptions_after))
        group_names = [s.group_name for s in subscriptions_after]
        self.assertNotIn(group_name, group_names)

        with self.assertRaises(NotFound):
            self.client.delete_subscription(group_name=group_name)

    def test_subscription_to_stream_from_start(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
        )

        # Read events from subscription.
        subscription = self.client.read_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
        )

        events = []
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event6.id:
                break

        # Check received events.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event4.id)
        self.assertEqual(events[1].id, event5.id)
        self.assertEqual(events[2].id, event6.id)

        # Append some more events.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=2, events=[event7, event8, event9]
        )

        event10 = NewEvent(type="OrderCreated", data=random_data())
        event11 = NewEvent(type="OrderUpdated", data=random_data())
        event12 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, current_version=2, events=[event10, event11, event12]
        )

        # Continue receiving events.
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event12.id:
                break

        # Check received events.
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event4.id)
        self.assertEqual(events[1].id, event5.id)
        self.assertEqual(events[2].id, event6.id)
        self.assertEqual(events[3].id, event10.id)
        self.assertEqual(events[4].id, event11.id)
        self.assertEqual(events[5].id, event12.id)

    def test_subscription_to_stream_from_stream_position(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
            stream_position=1,
        )

        # Read events from subscription.
        subscription = self.client.read_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
        )

        events = []
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event6.id:
                break

        # Check received events.
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event5.id)
        self.assertEqual(events[1].id, event6.id)

        # Append some more events.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=2, events=[event7, event8, event9]
        )

        event10 = NewEvent(type="OrderCreated", data=random_data())
        event11 = NewEvent(type="OrderUpdated", data=random_data())
        event12 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, current_version=2, events=[event10, event11, event12]
        )

        # Continue receiving events.
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event12.id:
                break

        # Check received events.
        self.assertEqual(len(events), 5)
        self.assertEqual(events[0].id, event5.id)
        self.assertEqual(events[1].id, event6.id)
        self.assertEqual(events[2].id, event10.id)
        self.assertEqual(events[3].id, event11.id)
        self.assertEqual(events[4].id, event12.id)

    def test_subscription_to_stream_from_end(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
            from_end=True,
        )

        # Read events from subscription.
        subscription = self.client.read_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
        )

        # Append some more events.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, current_version=2, events=[event7, event8, event9]
        )

        event10 = NewEvent(type="OrderCreated", data=random_data())
        event11 = NewEvent(type="OrderUpdated", data=random_data())
        event12 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, current_version=2, events=[event10, event11, event12]
        )

        # Receive events from subscription.
        events = []
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            if event.id == event12.id:
                break

        # Check received events.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event10.id)
        self.assertEqual(events[1].id, event11.id)
        self.assertEqual(events[2].id, event12.id)

    def test_subscription_to_stream_with_consumer_strategy_round_robin(
        self,
    ) -> None:
        self.construct_esdb_client()

        stream_name1 = str(uuid4())

        # Create persistent subscription.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name1,
            stream_name=stream_name1,
            consumer_strategy="RoundRobin",
        )

        # Multiple consumers.
        subscription1 = self.client.read_subscription_to_stream(
            group_name=group_name1, stream_name=stream_name1
        )
        subscription2 = self.client.read_subscription_to_stream(
            group_name=group_name1, stream_name=stream_name1
        )

        # Append three events.
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event4 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3, event4],
        )

        events1 = []
        events2 = []
        while True:
            event = next(subscription1)
            subscription1.ack(event)
            events1.append(event)

            event = next(subscription2)
            subscription2.ack(event)
            events2.append(event)
            event_ids = {e.id for e in events1 + events2}
            if event4.id in event_ids:
                break

        # Check events have been distributed evenly.
        # NB: this only works if events are appended after consumers have started,
        # otherwise some events are sent to both, and I'm not sure what would happen
        # if consumers stop and are restarted.
        len1 = len(events1)
        len2 = len(events2)

        self.assertEqual(len1, 2)
        self.assertEqual(events1[0].id, event1.id)
        self.assertEqual(events1[1].id, event3.id)

        self.assertEqual(len2, 2)
        self.assertEqual(events2[0].id, event2.id)
        self.assertEqual(events2[1].id, event4.id)

    def test_subscription_to_stream_can_be_stopped(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2, event3],
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4, event5, event6],
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
        )

        # Read subscription.
        subscription = self.client.read_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name2,
        )

        # Stop subscription.
        subscription.stop()

        # Check we receive zero events.
        events = list(subscription)
        self.assertEqual(0, len(events))

    def test_subscription_to_stream_get_info(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())
        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(NotFound):
            self.client.get_subscription_info(
                group_name=group_name,
                stream_name=stream_name,
            )

        # Create persistent stream subscription.
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        info = self.client.get_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
        )
        self.assertEqual(info.group_name, group_name)

    def test_stream_subscriptions_list(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())

        subscriptions_before = self.client.list_subscriptions_to_stream(stream_name)
        self.assertEqual(subscriptions_before, [])

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        subscriptions_after = self.client.list_subscriptions_to_stream(stream_name)
        self.assertEqual(len(subscriptions_after), 1)
        self.assertEqual(subscriptions_after[0].group_name, group_name)

        # Actually, stream subscription also appears in "list_subscriptions()"?
        all_subscriptions = self.client.list_subscriptions()
        group_names = [s.group_name for s in all_subscriptions]
        self.assertIn(group_name, group_names)

    def test_subscription_to_stream_update(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"

        # Can't update a subscription that doesn't exist.
        with self.assertRaises(NotFound):
            self.client.update_subscription_to_stream(
                group_name=group_name, stream_name=stream_name
            )

        # Create persistent subscription.
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        # Can update a subscription that does exist.
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        info = self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")

        # Update subscription to run from end.
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, from_end=True
        )
        info = self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "-1")

        # Update subscription to run from stream position.
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, stream_position=10
        )
        info = self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "10")

        # Update subscription to run from start.
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )
        info = self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(info.start_from, "0")

        # Update subscription to resolve links.
        self.assertFalse(info.resolve_link_tos)
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, resolve_links=True
        )
        info = self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertTrue(info.resolve_link_tos)

        # Update subscription's max_subscriber_count.
        self.assertEqual(5, info.max_subscriber_count)
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_subscriber_count=10
        )
        info = self.client.get_subscription_info(
            group_name=group_name, stream_name=stream_name
        )
        self.assertEqual(10, info.max_subscriber_count)

    def test_subscription_to_stream_message_timeout_setting(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"

        # Create persistent subscription.
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            message_timeout=12.345,
        )

        info = self.client.get_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
        )
        self.assertEqual(info.message_timeout_milliseconds, 12345)

        # Update subscription.
        self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            message_timeout=54.321,
        )

        info = self.client.get_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
        )
        self.assertEqual(info.message_timeout_milliseconds, 54321)

    def test_subscription_to_stream_max_retry_count_setting(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"

        # Create persistent subscription.
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            max_retry_count=60,
        )

        info = self.client.get_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
        )
        self.assertEqual(info.max_retry_count, 60)

        # Update subscription.
        self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            max_retry_count=1,
        )

        info = self.client.get_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
        )
        self.assertEqual(info.max_retry_count, 1)

    def test_subscription_to_stream_max_subscriber_count(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-group-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_subscriber_count=2
        )

        # Multiple consumers.
        subscription1 = self.client.read_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )
        subscription2 = self.client.read_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )
        with self.assertRaises(MaximumSubscriptionsReached):
            self.client.read_subscription_to_stream(
                group_name=group_name, stream_name=stream_name
            )
        subscription1.stop()
        subscription2.stop()

    def test_subscription_to_stream_already_exists(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"

        # Create persistent subscription.
        self.client.create_subscription_to_stream(group_name, stream_name)

        # Try to create same persistent subscription.
        with self.assertRaises(AlreadyExists):
            self.client.create_subscription_to_stream(group_name, stream_name)

    def test_subscription_to_stream_delete(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())
        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(NotFound):
            self.client.delete_subscription(
                group_name=group_name, stream_name=stream_name
            )

        # Create persistent stream subscription.
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        subscriptions_before = self.client.list_subscriptions_to_stream(stream_name)
        self.assertEqual(len(subscriptions_before), 1)

        self.client.delete_subscription(group_name=group_name, stream_name=stream_name)

        subscriptions_after = self.client.list_subscriptions_to_stream(stream_name)
        self.assertEqual(len(subscriptions_after), 0)

        with self.assertRaises(NotFound):
            self.client.delete_subscription(
                group_name=group_name, stream_name=stream_name
            )

    # Todo: consumer_strategy, RoundRobin and Pinned, need to test with more than
    #  one consumer, also code this as enum rather than a string
    # Todo: Nack? exception handling on callback?
    # Todo: update subscription
    # Todo: filter options
    # Todo: subscribe from end? not interesting, because you can get commit position

    def test_subscription_to_stream_resolve_links(self) -> None:
        if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")
        self.construct_esdb_client()

        event_type = "EventType" + str(uuid4()).replace("-", "")[:5]

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=f"$et-{event_type}",
            resolve_links=True,
        )

        # Append some events.
        stream_name1 = str(uuid4())

        # NB only events with JSON data are projected into "$et-{event_type}" streams.
        event1 = NewEvent(type=event_type, data=b"{}")
        event2 = NewEvent(type=event_type, data=b"{}")
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )

        sleep(1)

        # Read events from subscription.
        subscription = self.client.read_subscription_to_stream(
            group_name=group_name,
            stream_name=f"$et-{event_type}",
        )

        events = []
        for event in subscription:
            subscription.ack(event)
            events.append(event)
            break

        # Check received events.
        self.assertEqual(len(events), 1)

    # def test_print_resolve_links(self) -> None:
    #     if self.ESDB_CLUSTER_SIZE > 1 or self.ESDB_TLS is not True:
    #         self.skipTest("This test doesn't work with this configuration")
    #     self.construct_esdb_client()
    #
    #     event_type = "EventType-" + str(uuid4()).replace("-", "")[:5]
    #     stream_name = str(uuid4())
    #
    #     # NB only events with JSON data are projected into "$et-{event_type}" streams.
    #     print("new event type")
    #     print(event_type)
    #     print()
    #     print("appended to new stream")
    #     event1 = NewEvent(
    #         id=uuid5(NAMESPACE_URL, "1" + event_type), type=event_type, data=b"{}"
    #     )
    #     event2 = NewEvent(
    #         id=uuid5(NAMESPACE_URL, "2" + event_type), type=event_type, data=b"{}"
    #     )
    #     print("Event1 ID:", event1.id)
    #     print("Event2 ID:", event2.id)
    #     print()
    #
    #     try:
    #         self.client.append_events(
    #             stream_name,
    #             current_version=StreamState.NO_STREAM,
    #             events=[event1, event2],
    #         )
    #     except WrongCurrentVersion:
    #         pass
    #
    #     event_type_stream_name = f"$et-{event_type}"
    #
    #     # Create three persistent stream subscriptions.
    #     # group_name1 = f"my-subscription-{uuid4().hex}"
    #     # group_name2 = f"my-subscription-{uuid4().hex}"
    #     # group_name3 = f"my-subscription-{uuid4().hex}"
    #     # self.client.create_subscription_to_stream(
    #     #     group_name=group_name1,
    #     #     stream_name=stream_name,
    #     #     resolve_links=False,
    #     # )
    #     # self.client.create_subscription_to_stream(
    #     #     group_name=group_name2,
    #     #     stream_name=event_type_stream_name,
    #     #     resolve_links=False,
    #     # )
    #     # self.client.create_subscription_to_stream(
    #     #     group_name=group_name3,
    #     #     stream_name=event_type_stream_name,
    #     #     resolve_links=True,
    #     # )
    #
    #     # subscription1 = self.client.read_subscription_to_stream(
    #     #     group_name=group_name1,
    #     #     stream_name=stream_name,
    #     # )
    #     # subscription2 = self.client.readption_to_stream(
    #     #     group_name=group_name2,
    #     #     stream_name=event_type_stream_name,
    #     # )
    #     # subscription3 = self.client.readption_to_stream(
    #     #     group_name=group_name3,
    #     #     stream_name=event_type_stream_name,
    #     # )
    #     subscription1 = self.client.read_stream(
    #         # group_name=group_name1,
    #         stream_name=stream_name,
    #     )
    #     subscription2 = self.client.read_stream(
    #         # group_name=group_name2,
    #         stream_name=event_type_stream_name,
    #     )
    #     subscription3 = self.client.read_stream(
    #         # group_name=group_name3,
    #         stream_name=event_type_stream_name,
    #         resolve_links=True,
    #     )
    #
    #     print("subscription to new stream")
    #     with subscription1:
    #         received1 = next(subscription1)
    #         received2 = next(subscription1)
    #         print("Event1 ID:", received1.id)
    #         print("Event1 type:", received1.type)
    #         print("Event1 link ID:", received1.link.id if received1.link else "")
    #         print("Event1 link type:", received1.link.type if received1.link else "")
    #         print("Event2 ID:", received2.id)
    #         print("Event2 type:", received2.type)
    #         print("Event2 link ID:", received2.link.id if received2.link else "")
    #         print("Event1 link type:", received2.link.type if received2.link else "")
    #         print()
    #
    #     print("subscription to event type stream: resolve_links=False")
    #     with subscription2:
    #         received1 = next(subscription2)
    #         received2 = next(subscription2)
    #         print("Event1 ID:", received1.id)
    #         print("Event1 type:", received1.type)
    #         print("Event1 link ID:", received1.link.id if received1.link else "")
    #         print("Event1 link type:", received1.link.type if received1.link else "")
    #         print("Event2 ID:", received2.id)
    #         print("Event2 type:", received2.type)
    #         print("Event2 link ID:", received2.link.id if received2.link else "")
    #         print("Event1 link type:", received2.link.type if received2.link else "")
    #         print()
    #
    #     print("subscription to event type stream: resolve_links=True")
    #     with subscription3:
    #         received1 = next(subscription3)
    #         received2 = next(subscription3)
    #         print("Event1 ID:", received1.id)
    #         print("Event1 type:", received1.type)
    #         print("Event1 link ID:", received1.link.id if received1.link else "")
    #         print("Event1 link type:", received1.link.type if received1.link else "")
    #         print("Event2 ID:", received2.id)
    #         print("Event2 type:", received2.type)
    #         print("Event2 link ID:", received2.link.id if received2.link else "")
    #         print("Event1 link type:", received2.link.type if received2.link else "")
    #         print()

    def test_stream_metadata_get_and_set(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Append batch of new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
        )
        self.assertEqual(2, len(self.client.get_stream(stream_name)))

        # Get stream metadata (should be empty).
        metadata, version = self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata, {})

        # Delete stream.
        self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)
        with self.assertRaises(NotFound):
            self.client.get_stream(stream_name)

        # Get stream metadata (should have "$tb").
        metadata, version = self.client.get_stream_metadata(stream_name)
        self.assertIsInstance(metadata, dict)
        self.assertIn("$tb", metadata)
        max_long = 9223372036854775807
        self.assertEqual(metadata["$tb"], max_long)

        # Set stream metadata.
        metadata["foo"] = "bar"
        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
            current_version=version,
        )

        # Check the metadata has "foo".
        metadata, version = self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata["foo"], "bar")

        # For some reason "$tb" is now (most often) 2 rather than max_long.
        # Todo: Why is this?
        self.assertIn(metadata["$tb"], [2, max_long])

        # Get and set metadata for a stream that does not exist.
        stream_name = str(uuid4())
        metadata, version = self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata, {})

        metadata["foo"] = "baz"
        self.client.set_stream_metadata(
            stream_name=stream_name, metadata=metadata, current_version=version
        )
        metadata, version = self.client.get_stream_metadata(stream_name)
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
        self.client.set_stream_metadata(
            stream_name, metadata=metadata, current_version=version
        )
        metadata, version = self.client.get_stream_metadata(stream_name)
        self.assertEqual(metadata["$acl"], acl)

        with self.assertRaises(WrongCurrentVersion):
            self.client.set_stream_metadata(
                stream_name=stream_name,
                metadata=metadata,
                current_version=10,
            )

        self.client.tombstone_stream(stream_name, current_version=StreamState.ANY)

        # Can't get metadata after tombstoning stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_metadata(stream_name)

        # For some reason, we can set stream metadata, even though the stream
        # has been tombstoned, and even though we can't get stream metadata.
        # Todo: Ask ESDB team why this is?
        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
            current_version=1,
        )

        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=metadata,
            current_version=StreamState.ANY,
        )

        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_metadata(stream_name)

    def test_gossip_read(self) -> None:
        self.construct_esdb_client()
        if self.ESDB_CLUSTER_SIZE == 1:
            cluster_info = self.client.read_gossip()
            self.assertEqual(len(cluster_info), 1)
            expected_address = self.ESDB_TARGET.split(":")[0]
            expected_port = int(self.ESDB_TARGET.split(":")[1])
            self.assertEqual(cluster_info[0].state, NODE_STATE_LEADER)
            self.assertEqual(cluster_info[0].address, expected_address)
            self.assertEqual(cluster_info[0].port, expected_port)
        elif self.ESDB_CLUSTER_SIZE == 3:
            retries = 10
            while True:
                retries -= 1
                try:
                    cluster_info = self.client.read_gossip()
                    self.assertEqual(len(cluster_info), 3)
                    num_leaders = 0
                    num_followers = 0
                    for member_info in cluster_info:
                        if member_info.state == NODE_STATE_LEADER:
                            num_leaders += 1
                        elif member_info.state == NODE_STATE_FOLLOWER:
                            num_followers += 1
                    self.assertEqual(num_leaders, 1)
                    # Todo: This is very occasionally 1... hence retries.
                    self.assertEqual(num_followers, 2)
                    break
                except AssertionError:
                    if retries:
                        sleep(1)
                    else:
                        raise
        else:
            self.fail(f"Test doesn't work with cluster size {self.ESDB_CLUSTER_SIZE}")

    # Getting 'AccessDenied' with ESDB v23.10.
    # def test_gossip_cluster_read(self) -> None:
    #     self.construct_esdb_client()
    #     if self.ESDB_CLUSTER_SIZE == 1:
    #         cluster_info = self.client.read_cluster_gossip()
    #         self.assertEqual(len(cluster_info), 1)
    #         self.assertEqual(cluster_info[0].state, NODE_STATE_LEADER)
    #         self.assertEqual(cluster_info[0].address, "127.0.0.1")
    #         self.assertEqual(cluster_info[0].port, 2113)
    #     elif self.ESDB_CLUSTER_SIZE == 3:
    #         retries = 10
    #         while True:
    #             retries -= 1
    #             try:
    #                 cluster_info = self.client.read_cluster_gossip()
    #                 self.assertEqual(len(cluster_info), 3)
    #                 num_leaders = 0
    #                 num_followers = 0
    #                 for member_info in cluster_info:
    #                     if member_info.state == NODE_STATE_LEADER:
    #                         num_leaders += 1
    #                     elif member_info.state == NODE_STATE_FOLLOWER:
    #                         num_followers += 1
    #                 self.assertEqual(num_leaders, 1)
    #                 # Todo: This is very occasionally 1... hence retries.
    #                 self.assertEqual(num_followers, 2)
    #                 break
    #             except AssertionError:
    #                 if retries:
    #                     sleep(1)
    #                 else:
    #                     raise
    #     else:
    #         self.fail(f"Test doesn't work with cluster size {self.ESDB_CLUSTER_SIZE}")


class TestEventStoreDBClientWithInsecureConnection(TestEventStoreDBClient):
    ESDB_TARGET = "localhost:2113"
    ESDB_TLS = False

    def test_raises_service_unavailable_exception(self) -> None:
        # Getting DeadlineExceeded as __cause__ with "insecure" server.
        pass

    def test_raises_discovery_failed_exception(self) -> None:
        super().test_raises_discovery_failed_exception()


class TestESDBClusterNode1(TestEventStoreDBClient):
    ESDB_TARGET = "127.0.0.1:2110"
    ESDB_CLUSTER_SIZE = 3


class TestESDBClusterNode2(TestEventStoreDBClient):
    ESDB_TARGET = "127.0.0.1:2111"
    ESDB_CLUSTER_SIZE = 3


class TestESDBClusterNode3(TestEventStoreDBClient):
    ESDB_TARGET = "127.0.0.1:2112"
    ESDB_CLUSTER_SIZE = 3


class TestESDBDiscoverScheme(TestCase):
    def test_calls_dns_and_uses_given_port_number_or_default(self) -> None:
        uri = (
            "esdb+discover://example.com"
            "?Tls=false&GossipTimeout=0&DiscoveryInterval=0&MaxDiscoverAttempts=2"
        )
        with self.assertRaises(DiscoveryFailed) as cm:
            EventStoreDBClient(uri)
        self.assertIn(":2113", str(cm.exception))

        uri = (
            "esdb+discover://example.com:9898"
            "?Tls=false&GossipTimeout=0&DiscoveryInterval=0&MaxDiscoverAttempts=2"
        )
        with self.assertRaises(DiscoveryFailed) as cm:
            EventStoreDBClient(uri)
        self.assertIn(":9898", str(cm.exception))

    def test_raises_dns_error(self) -> None:
        with self.assertRaises(DNSError):
            EventStoreDBClient("esdb+discover://xxxxxxxxxxxxxx")


class TestGrpcOptions(TestCase):
    def setUp(self) -> None:
        uri = (
            "esdb://localhost:2113"
            "?Tls=false&KeepAliveInterval=1000&KeepAliveTimeout=1000"
        )
        self.client = EventStoreDBClient(uri)

    def tearDown(self) -> None:
        self.client.close()

    def test(self) -> None:
        self.assertEqual(
            self.client.grpc_options["grpc.max_receive_message_length"],
            17 * 1024 * 1024,
        )
        self.assertEqual(self.client.grpc_options["grpc.keepalive_ms"], 1000)
        self.assertEqual(self.client.grpc_options["grpc.keepalive_timeout_ms"], 1000)


class TestRequiresLeaderHeader(TimedTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uri = "esdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111,127.0.0.1:2112"
        self.ca_cert = get_ca_certificate()
        self.writer = EventStoreDBClient(
            self.uri + "?NodePreference=leader", root_certificates=self.ca_cert
        )
        self.reader = EventStoreDBClient(
            self.uri + "?NodePreference=follower", root_certificates=self.ca_cert
        )

    def tearDown(self) -> None:
        self.writer.close()
        self.reader.close()
        super().tearDown()

    def test_can_subscribe_to_all_on_follower(self) -> None:
        # Subscribe to follower.
        subscription = self.reader.subscribe_to_all(timeout=10, from_end=True)

        # Write to leader.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.writer.append_events(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )
        # Read from follower.
        for recorded_event in subscription:
            if recorded_event.id == event2.id:
                break

    def test_can_subscribe_to_stream_on_follower(self) -> None:
        # Write to leader.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.writer.append_events(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )
        # Read from follower.
        for recorded_event in self.reader.subscribe_to_stream(stream_name, timeout=5):
            if recorded_event.id == event2.id:
                break

    def _set_reader_connection_on_writer(self) -> None:
        # Give the writer a connection to a follower.
        old, self.writer._esdb = self.writer._esdb, self.reader._esdb
        # - this is hopeful mitigation for the "Exception was thrown by handler"
        #   which is occasionally a cause of failure of test_append_events()
        #   with both EventStoreDB 21.10.9 and 22.10.0.
        old.close()
        # sleep(0.1)
        sleep(1)

    def test_reconnects_to_new_leader_on_append_event(self) -> None:
        # Fail to write to follower.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeader):
            self.reader.append_event(
                stream_name, current_version=StreamState.NO_STREAM, event=event1
            )

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and write to leader.
        self.writer.append_event(
            stream_name, current_version=StreamState.NO_STREAM, event=event1
        )

    def test_reconnects_to_new_leader_on_append_events(self) -> None:
        # Fail to write to follower.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        with self.assertRaises(NodeIsNotLeader):
            self.reader.append_events(
                stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event1, event2],
            )

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Todo: Occasionally getting "Exception was thrown by handler." from this. Why?
        #   esdbclient.exceptions.ExceptionThrownByHandler: <_MultiThreadedRendezvous of
        #   RPC that terminated with:
        #       status = StatusCode.UNKNOWN
        #       details = "Exception was thrown by handler."
        #       debug_error_string = "UNKNOWN:Error received from peer  {grpc_message:"
        #       Exception was thrown by handler.", grpc_status:2, created_time:"2023-05
        #       -07T12:04:26.287327771+00:00"}"
        retries = 1
        while retries:
            retries -= 1

            # Reconnect and write to leader.
            try:
                self.writer.append_events(
                    stream_name,
                    current_version=StreamState.NO_STREAM,
                    events=[event1, event2],
                )
            except ExceptionThrownByHandler:
                if retries == 0:
                    raise
                else:
                    sleep(1)
            else:
                break

    def test_reconnects_to_new_leader_on_set_stream_metadata(self) -> None:
        # Fail to write to follower.
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeader):
            self.reader.set_stream_metadata(stream_name=stream_name, metadata={})

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and write to leader.
        self.writer.set_stream_metadata(stream_name=stream_name, metadata={})

    def test_reconnects_to_new_leader_on_delete_stream(self) -> None:
        # Need to append some events before deleting stream...
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.writer.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
        )

        # Fail to delete stream on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.delete_stream(stream_name, current_version=1)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Delete stream on leader.
        self.writer.delete_stream(stream_name, current_version=1)

    def test_reconnects_to_new_leader_on_tombstone_stream(self) -> None:
        # Fail to tombstone stream on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.tombstone_stream(str(uuid4()), current_version=StreamState.ANY)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Tombstone stream on leader.
        self.writer.tombstone_stream(str(uuid4()), current_version=StreamState.ANY)

    def test_reconnects_to_new_leader_on_create_subscription_to_all(self) -> None:
        # Fail to create subscription on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.create_subscription_to_all(group_name=f"group{str(uuid4())}")

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Create subscription on leader.
        self.writer.create_subscription_to_all(group_name=f"group{str(uuid4())}")

    def test_reconnects_to_new_leader_on_create_subscription_to_stream(self) -> None:
        # Fail to create subscription on follower.
        group_name = f"group{str(uuid4())}"
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeader):
            self.reader.create_subscription_to_stream(
                group_name=group_name, stream_name=stream_name
            )

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Create subscription on leader.
        self.writer.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

    def test_reconnects_to_new_leader_on_read_subscription_to_all(self) -> None:
        # Create subscription on leader.
        group_name = f"group{str(uuid4())}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to read subscription on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.read_subscription_to_all(group_name=group_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and read subscription on leader.
        self.writer.read_subscription_to_all(group_name=group_name)

    def test_reconnects_to_new_leader_on_read_subscription_to_stream(self) -> None:
        # Create stream subscription on leader.
        group_name = f"group{str(uuid4())}"
        stream_name = str(uuid4())
        self.writer.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        # Fail to read stream subscription on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.read_subscription_to_stream(
                group_name=group_name, stream_name=stream_name
            )

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and read stream subscription on leader.
        self.writer.read_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

    def test_reconnects_to_new_leader_on_list_subscriptions(self) -> None:
        # Create subscription on leader.
        group_name = f"group{str(uuid4())}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to list subscriptions on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.list_subscriptions()

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and list subscriptions on leader.
        self.writer.list_subscriptions()

    def test_reconnects_to_new_leader_on_list_subscriptions_to_stream(self) -> None:
        # Create stream subscription on leader.
        group_name = f"group{str(uuid4())}"
        stream_name = str(uuid4())
        self.writer.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        # Fail to list stream subscriptions on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.list_subscriptions_to_stream(stream_name=stream_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and list stream subscriptions on leader.
        self.writer.list_subscriptions_to_stream(stream_name=stream_name)

    def test_reconnects_to_new_leader_on_get_subscription_info(self) -> None:
        # Create subscription on leader.
        group_name = f"group{str(uuid4())}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to get subscription info on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.get_subscription_info(group_name=group_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and get subscription info on leader.
        self.writer.get_subscription_info(group_name=group_name)

    def test_reconnects_to_new_leader_on_delete_subscription(self) -> None:
        # Create subscription on leader.
        group_name = f"group{str(uuid4())}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to delete subscription on follower.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.delete_subscription(group_name=group_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and delete subscription on leader.
        self.writer.delete_subscription(group_name=group_name)

    def test_reconnects_to_leader_on_read_stream_when_node_preference_is_leader(
        self,
    ) -> None:
        # Append some events.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.writer.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1, event2]
        )

        # Make sure the reader has the events.
        while True:
            try:
                stream_events = self.reader.get_stream(stream_name)
            except NotFound:
                pass
            else:
                if len(stream_events) == 2:
                    break
            sleep(0.1)

        # Change reader's node preference to 'leader'
        self.reader.connection_spec.options._NodePreference = "leader"

        # Check reader reconnects to leader.
        self.assertNotEqual(
            self.reader._esdb._grpc_target, self.writer._esdb._grpc_target
        )
        connection_id = id(self.reader._esdb)
        self.reader.get_stream(stream_name)
        self.assertNotEqual(connection_id, id(self.reader._esdb))
        self.assertEqual(self.reader._esdb._grpc_target, self.writer._esdb._grpc_target)


class TestAutoReconnectClosedConnection(TimedTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uri = "esdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111,127.0.0.1:2112"
        self.ca_cert = get_ca_certificate()
        self.writer = EventStoreDBClient(
            self.uri + "?NodePreference=leader", root_certificates=self.ca_cert
        )
        self.writer.close()

    def tearDown(self) -> None:
        self.writer.close()
        super().tearDown()

    def test_append_events(self) -> None:
        # Append events - should reconnect.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        self.writer.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event1]
        )

    def test_get_stream(self) -> None:
        # Read all events - should reconnect.
        with self.assertRaises(NotFound):
            self.writer.get_stream(str(uuid4()))

    def test_read_subscription_to_all(self) -> None:
        # Read subscription - should reconnect.
        with self.assertRaises(NotFound):
            self.writer.read_subscription_to_all(str(uuid4()))


class TestAutoReconnectAfterServiceUnavailable(TimedTestCase):
    def setUp(self) -> None:
        super().setUp()
        uri = "esdb://admin:changeit@localhost:2114?MaxDiscoverAttempts=1&DiscoveryInterval=0"
        server_certificate = get_server_certificate("localhost:2114")
        self.client = EventStoreDBClient(uri=uri, root_certificates=server_certificate)

        # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
        self.client._esdb.close()
        self.client._esdb = self.client._construct_esdb_connection("localhost:2222")

    def tearDown(self) -> None:
        self.client.close()
        super().tearDown()

    def test_append_events(self) -> None:
        self.client.append_events(
            str(uuid4()),
            current_version=StreamState.NO_STREAM,
            events=[NewEvent(type="X", data=b"")],
        )

    def test_get_stream(self) -> None:
        with self.assertRaises(NotFound):
            self.client.get_stream(
                str(uuid4()),
            )

    def test_read_stream(self) -> None:
        read_response = self.client.read_stream(
            str(uuid4()),
        )
        with self.assertRaises(ServiceUnavailable):
            tuple(read_response)

    def test_read_all(self) -> None:
        read_response = self.client.read_all()
        with self.assertRaises(ServiceUnavailable):
            tuple(read_response)

    def test_append_event(self) -> None:
        self.client.append_event(
            str(uuid4()),
            current_version=StreamState.NO_STREAM,
            event=NewEvent(type="X", data=b""),
        )
        self.client.append_event(
            str(uuid4()),
            current_version=StreamState.NO_STREAM,
            event=NewEvent(type="X", data=b""),
        )

    def test_create_subscription_to_all(self) -> None:
        self.client.create_subscription_to_all(
            group_name=f"my-subscription-{uuid4().hex}"
        )

    def test_create_subscription_to_stream(self) -> None:
        self.client.create_subscription_to_stream(
            group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
        )

    def test_subscribe_to_all(self) -> None:
        self.client.subscribe_to_all()

    def test_subscribe_to_stream(self) -> None:
        self.client.subscribe_to_stream(str(uuid4()))

    def test_get_subscription_info(self) -> None:
        with self.assertRaises(NotFound):
            self.client.get_subscription_info(
                group_name=f"my-subscription-{uuid4().hex}"
            )

    def test_list_subscriptions(self) -> None:
        self.client.list_subscriptions()

    def test_list_subscriptions_to_stream(self) -> None:
        self.client.list_subscriptions_to_stream(stream_name=str(uuid4()))

    def test_delete_stream(self) -> None:
        with self.assertRaises(NotFound):
            self.client.delete_stream(
                stream_name=str(uuid4()), current_version=StreamState.NO_STREAM
            )

    def test_replay_parked_events(self) -> None:
        with self.assertRaises(NotFound):
            self.client.replay_parked_events(
                group_name=f"my-subscription-{uuid4().hex}"
            )

        with self.assertRaises(NotFound):
            self.client.replay_parked_events(
                group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
            )

    def test_delete_subscription(self) -> None:
        with self.assertRaises(NotFound):
            self.client.delete_subscription(group_name=f"my-subscription-{uuid4().hex}")

        with self.assertRaises(NotFound):
            self.client.delete_subscription(
                group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
            )

    def test_read_gossip(self) -> None:
        self.client.read_gossip()

    # Getting 'AccessDenied' with ESDB v23.10.
    # def test_read_cluster_gossip(self) -> None:
    #     self.client.read_cluster_gossip()


class TestConnectToPreferredNode(TimedTestCase):
    def test_no_followers(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2113?Tls=false&NodePreference=follower"
        with self.assertRaises(FollowerNotFound):
            EventStoreDBClient(uri)

    def test_no_read_only_replicas(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2113?Tls=false&NodePreference=readonlyreplica"
        with self.assertRaises(ReadOnlyReplicaNotFound):
            EventStoreDBClient(uri)

    def test_random(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2113?Tls=false&NodePreference=random"
        EventStoreDBClient(uri)


class TestSubscriptionReadRequest(TimedTestCase):
    def test_request_ack_after_100_acks(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_ids: List[UUID] = []
        for _ in range(102):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.ack(event_id)
        sleep(read_request._max_ack_delay)
        grpc_read_req1 = next(read_request_iter)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req1.ack.ids), 1)
        self.assertEqual(len(grpc_read_req1.nack.ids), 0)
        self.assertIsInstance(grpc_read_req2, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req2.ack.ids), 100)
        self.assertEqual(len(grpc_read_req2.nack.ids), 0)
        self.assertIsInstance(grpc_read_req3, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req3.ack.ids), 1)
        self.assertEqual(len(grpc_read_req3.nack.ids), 0)

    def test_request_nack_after_100_nacks(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_ids: List[UUID] = []
        for _ in range(102):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.nack(event_id, "park")
        sleep(read_request._max_ack_delay)
        grpc_read_req1 = next(read_request_iter)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertIsInstance(grpc_read_req2, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 100)
        self.assertIsInstance(grpc_read_req3, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 1)

    def test_request_ack_ack_ack(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 100)

        # Do three acks.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        read_request.ack(event_id2)
        read_request.ack(event_id3)
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req2.ack.ids), 1)
        self.assertEqual(len(grpc_read_req2.nack.ids), 0)
        self.assertEqual(len(grpc_read_req3.ack.ids), 2)
        self.assertEqual(len(grpc_read_req3.nack.ids), 0)
        self.assertEqual(grpc_read_req2.ack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req3.ack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req3.ack.ids[1].string, str(event_id3))

    def test_request_nack_unknown_after_max_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 100)

        # Do three nack unknown.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "unknown")
        read_request.nack(event_id2, "unknown")
        read_request.nack(event_id3, "unknown")
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 2)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req3.nack.ids[1].string, str(event_id3))
        self.assertEqual(
            grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Unknown
        )
        self.assertEqual(
            grpc_read_req3.nack.action, persistent_pb2.ReadReq.Nack.Unknown
        )

    def test_request_nack_park_after_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 100)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        read_request.nack(event_id2, "park")
        read_request.nack(event_id3, "park")
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 2)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req3.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Park)
        self.assertEqual(grpc_read_req3.nack.action, persistent_pb2.ReadReq.Nack.Park)

    def test_request_nack_retry_after_max_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 100)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "retry")
        read_request.nack(event_id2, "retry")
        read_request.nack(event_id3, "retry")
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 2)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req3.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Retry)
        self.assertEqual(grpc_read_req3.nack.action, persistent_pb2.ReadReq.Nack.Retry)

    def test_request_nack_skip_after_100ms(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 100)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "skip")
        read_request.nack(event_id2, "skip")
        read_request.nack(event_id3, "skip")
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 2)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req3.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Skip)
        self.assertEqual(grpc_read_req3.nack.action, persistent_pb2.ReadReq.Nack.Skip)

    def test_request_nack_stop_after_max_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 100)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "stop")
        read_request.nack(event_id2, "stop")
        read_request.nack(event_id3, "stop")
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 2)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req3.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Stop)
        self.assertEqual(grpc_read_req3.nack.action, persistent_pb2.ReadReq.Nack.Stop)

    def test_request_ack_ack_nack(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        read_request.ack(event_id2)
        read_request.nack(event_id3, "park")

        sleep(read_request._max_ack_delay)

        grpc_read_req1 = next(read_request_iter)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req1.ack.ids), 1)
        self.assertEqual(len(grpc_read_req1.nack.ids), 0)
        self.assertEqual(grpc_read_req1.ack.ids[0].string, str(event_id1))

        self.assertEqual(len(grpc_read_req2.ack.ids), 1)
        self.assertEqual(len(grpc_read_req2.nack.ids), 0)
        self.assertEqual(grpc_read_req2.ack.ids[0].string, str(event_id2))

        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 1)
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id3))

    def test_request_nack_nack_ack(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        read_request.nack(event_id2, "park")
        read_request.ack(event_id3)

        sleep(read_request._max_ack_delay)

        grpc_read_req1 = next(read_request_iter)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))

        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id2))

        self.assertEqual(len(grpc_read_req3.ack.ids), 1)
        self.assertEqual(len(grpc_read_req3.nack.ids), 0)
        self.assertEqual(grpc_read_req3.ack.ids[0].string, str(event_id3))

    def test_request_nack_after_nack_followed_by_nack_with_other_action(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        read_request.nack(event_id2, "park")
        read_request.nack(event_id3, "skip")

        sleep(read_request._max_ack_delay)

        grpc_read_req1 = next(read_request_iter)
        grpc_read_req2 = next(read_request_iter)
        grpc_read_req3 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req1.nack.action, persistent_pb2.ReadReq.Nack.Park)

        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Park)

        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 1)
        self.assertEqual(grpc_read_req3.nack.ids[0].string, str(event_id3))
        self.assertEqual(grpc_read_req3.nack.action, persistent_pb2.ReadReq.Nack.Skip)

    def test_request_iter_stop(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        read_request.ack(event_id2)
        read_request.ack(event_id3)
        thread = Thread(target=read_request.stop)
        thread.start()

        sleep(read_request._max_ack_delay)

        while True:
            try:
                grpc_read_req1 = next(read_request_iter)
            except StopIteration:
                break

        thread.join()

        self.assertEqual(len(grpc_read_req1.ack.ids), 2)
        self.assertEqual(len(grpc_read_req1.nack.ids), 0)


class TestHandleRpcError(TestCase):
    def test_handle_exception_thrown_by_handler(self) -> None:
        with self.assertRaises(ExceptionThrownByHandler):
            raise handle_rpc_error(FakeExceptionThrownByHandlerError()) from None

    def test_handle_deadline_exceeded_error(self) -> None:
        with self.assertRaises(GrpcDeadlineExceeded):
            raise handle_rpc_error(FakeDeadlineExceededRpcError()) from None

    def test_handle_unavailable_error(self) -> None:
        with self.assertRaises(ServiceUnavailable):
            raise handle_rpc_error(FakeUnavailableRpcError()) from None

    def test_handle_writing_to_follower_error(self) -> None:
        with self.assertRaises(NodeIsNotLeader):
            raise handle_rpc_error(FakeWritingToFollowerError()) from None

    def test_handle_consumer_too_slow_error(self) -> None:
        with self.assertRaises(ConsumerTooSlow):
            raise handle_rpc_error(FakeConsumerTooSlowError()) from None

    def test_handle_aborted_by_server_error(self) -> None:
        with self.assertRaises(AbortedByServer):
            raise handle_rpc_error(FakeAbortedByServerError()) from None

    def test_handle_other_call_error(self) -> None:
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(FakeUnknownRpcError()) from None
        self.assertEqual(GrpcError, cm.exception.__class__)

    def test_handle_non_call_rpc_error(self) -> None:
        # Check non-Call errors are handled.
        class MyRpcError(RpcError):
            pass

        msg = "some non-Call error"
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(MyRpcError(msg)) from None
        self.assertEqual(cm.exception.__class__, GrpcError)
        self.assertIsInstance(cm.exception.args[0], MyRpcError)


class FakeRpcError(_MultiThreadedRendezvous):
    def __init__(self, status_code: StatusCode, details: str = "") -> None:
        super().__init__(
            state=_RPCState(
                due=[],
                initial_metadata=None,
                trailing_metadata=None,
                code=status_code,
                details=details,
            ),
            call=IntegratedCall(None, None),
            response_deserializer=lambda x: x,
            deadline=None,
        )


class FakeExceptionThrownByHandlerError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(
            status_code=StatusCode.UNKNOWN, details="Exception was thrown by handler."
        )


class FakeDeadlineExceededRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.DEADLINE_EXCEEDED)


class FakeUnavailableRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.UNAVAILABLE)


class FakeWritingToFollowerError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(
            status_code=StatusCode.NOT_FOUND, details="Leader info available"
        )


class FakeConsumerTooSlowError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.ABORTED, details="Consumer too slow")


class FakeAbortedByServerError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.ABORTED, details="")


class FakeUnknownRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.UNKNOWN)


def random_data() -> bytes:
    return os.urandom(16)
