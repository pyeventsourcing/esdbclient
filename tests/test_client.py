# -*- coding: utf-8 -*-
import os
import ssl
from time import sleep
from typing import Any, List, Tuple, cast
from unittest import TestCase
from unittest.case import _AssertRaisesContext
from uuid import UUID, uuid4

from grpc import RpcError, StatusCode
from grpc._channel import _MultiThreadedRendezvous, _RPCState
from grpc._cython.cygrpc import IntegratedCall

import esdbclient.protos.Grpc.persistent_pb2 as grpc_persistent
from esdbclient.client import ESDBClient
from esdbclient.connection import (
    NODE_PREFERENCE_FOLLOWER,
    NODE_PREFERENCE_LEADER,
    ConnectionSpec,
)
from esdbclient.esdbapi import (
    NODE_STATE_FOLLOWER,
    NODE_STATE_LEADER,
    SubscriptionReadRequest,
    handle_rpc_error,
)
from esdbclient.events import NewEvent
from esdbclient.exceptions import (
    DeadlineExceeded,
    DiscoveryFailed,
    ExceptionThrownByHandler,
    FollowerNotFound,
    GossipSeedError,
    GrpcError,
    NodeIsNotLeader,
    ReadOnlyReplicaNotFound,
    ServiceUnavailable,
    StreamIsDeleted,
    StreamNotFound,
    SubscriptionNotFound,
    WrongExpectedPosition,
)
from esdbclient.protos.Grpc import persistent_pb2


class TestConnectionSpec(TestCase):
    def test_scheme_and_netloc(self) -> None:
        spec = ConnectionSpec("esdb://host1:2111")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "host1:2111")
        self.assertEqual(spec.targets, ["host1:2111"])
        self.assertEqual(spec.username, None)
        self.assertEqual(spec.password, None)

        spec = ConnectionSpec("esdb://admin:changeit@host1:2111")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "admin:changeit@host1:2111")
        self.assertEqual(spec.targets, ["host1:2111"])
        self.assertEqual(spec.username, "admin")
        self.assertEqual(spec.password, "changeit")

        spec = ConnectionSpec("esdb://host1:2111,host2:2112,host3:2113")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "host1:2111,host2:2112,host3:2113")
        self.assertEqual(spec.targets, ["host1:2111", "host2:2112", "host3:2113"])
        self.assertEqual(spec.username, None)
        self.assertEqual(spec.password, None)

        spec = ConnectionSpec("esdb://admin:changeit@host1:2111,host2:2112,host3:2113")
        self.assertEqual(spec.scheme, "esdb")
        self.assertEqual(spec.netloc, "admin:changeit@host1:2111,host2:2112,host3:2113")
        self.assertEqual(spec.targets, ["host1:2111", "host2:2112", "host3:2113"])
        self.assertEqual(spec.username, "admin")
        self.assertEqual(spec.password, "changeit")

        spec = ConnectionSpec("esdb+discover://host1:2111")
        self.assertEqual(spec.scheme, "esdb+discover")
        self.assertEqual(spec.netloc, "host1:2111")
        self.assertEqual(spec.targets, ["host1:2111"])
        self.assertEqual(spec.username, None)
        self.assertEqual(spec.password, None)

        spec = ConnectionSpec("esdb+discover://admin:changeit@host1:2111")
        self.assertEqual(spec.scheme, "esdb+discover")
        self.assertEqual(spec.netloc, "admin:changeit@host1:2111")
        self.assertEqual(spec.targets, ["host1:2111"])
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


def read_ca_cert() -> str:
    ca_cert_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "certs/ca/ca.crt"
    )
    with open(ca_cert_path, "r") as f:
        return f.read()


def get_server_certificate(grpc_target: str) -> str:
    return ssl.get_server_certificate(
        addr=cast(Tuple[str, int], grpc_target.split(":")),
    )


class TestESDBClient(TestCase):
    client: ESDBClient

    ESDB_TARGET = "localhost:2115"
    ESDB_TLS = True
    ESDB_CLUSTER_SIZE = 1

    def construct_esdb_client(self) -> None:
        qs = "MaxDiscoverAttempts=3&DiscoveryInterval=100"
        if self.ESDB_TLS:
            uri = f"esdb://admin:changeit@{self.ESDB_TARGET}?{qs}"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"esdb://{self.ESDB_TARGET}?Tls=false&{qs}"
            root_certificates = None
        self.client = ESDBClient(uri, root_certificates=root_certificates)

    def get_root_certificates(self) -> str:
        if self.ESDB_CLUSTER_SIZE == 1:
            return get_server_certificate(self.ESDB_TARGET)
        elif self.ESDB_CLUSTER_SIZE == 3:
            return read_ca_cert()
        else:
            raise ValueError(
                f"Test doesn't work with cluster size {self.ESDB_CLUSTER_SIZE}"
            )

    def tearDown(self) -> None:
        if hasattr(self, "client"):
            self.client.close()

    def test_close(self) -> None:
        self.construct_esdb_client()
        self.client.close()
        self.client.close()

        self.construct_esdb_client()
        self.client.close()
        self.client.close()

    def test_constructor_raises_value_errors(self) -> None:
        # Secure URI without root_certificates.
        with self.assertRaises(ValueError) as cm0:
            ESDBClient("esdb://localhost:2222")
        self.assertIn(
            "root_certificates is required for secure connection",
            cm0.exception.args[0],
        )

        # Scheme must be 'esdb'.
        with self.assertRaises(ValueError) as cm1:
            ESDBClient(uri="http://localhost:2222")
        self.assertIn("Invalid URI scheme:", cm1.exception.args[0])

    def test_constructor_raises_gossip_seed_error(self) -> None:
        # Needs at least one target.
        with self.assertRaises(GossipSeedError):
            ESDBClient(uri="esdb://")

    def test_raises_discovery_failed_exception(self) -> None:
        # Reconstruct connection with wrong port.
        esdb_target = "localhost:2222"

        if self.ESDB_TLS:
            uri = f"esdb://admin:changeit@{esdb_target}?MaxDiscoverAttempts=2"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"esdb://{esdb_target}?Tls=false&MaxDiscoverAttempts=2"
            root_certificates = None

        with self.assertRaises(DiscoveryFailed):
            ESDBClient(uri, root_certificates=root_certificates)

    def test_connects_despite_bad_target_in_gossip_seed(self) -> None:
        # Reconstruct connection with wrong port.
        esdb_target = f"localhost:2222,{self.ESDB_TARGET}"

        if self.ESDB_TLS:
            uri = f"esdb://admin:changeit@{esdb_target}"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"esdb://{esdb_target}?Tls=false"
            root_certificates = None

        try:
            client = ESDBClient(uri, root_certificates=root_certificates)
        except Exception:
            self.fail("Failed to connect")
        else:
            client.close()

    def test_raises_service_unavailable_exception(self) -> None:
        self.construct_esdb_client()

        # Reconstruct connection with wrong port.
        self.client._connection.close()
        self.client._connection = self.client._construct_connection("localhost:2222")
        # self.client.connection_spec._targets = ["localhost:2222"]

        cm: _AssertRaisesContext[Any]

        resp = self.client.read_stream_events(str(uuid4()))
        with self.assertRaises(ServiceUnavailable) as cm:
            list(resp)
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        resp = self.client.read_all_events()
        with self.assertRaises(ServiceUnavailable) as cm:
            list(resp)
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        group_name = f"my-subscription-{uuid4().hex}"
        read_req, read_resp = self.client.read_subscription(
            group_name=group_name,
        )
        with self.assertRaises(ServiceUnavailable) as cm:
            list(read_resp)
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        group_name = f"my-subscription-{uuid4().hex}"
        read_req, read_resp = self.client.read_stream_subscription(
            group_name=group_name, stream_name=str(uuid4())
        )
        with self.assertRaises(ServiceUnavailable) as cm:
            list(read_resp)
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

    def test_stream_not_found_exception_from_read_stream_events(self) -> None:
        # Note, we never get a StreamNotFound from subscribe_stream_events(), which is
        # logical because the stream might be written after the subscription. So here
        # we just test read_stream_events().

        self.construct_esdb_client()
        stream_name = str(uuid4())

        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name, backwards=True))

        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name, stream_position=1))

        with self.assertRaises(StreamNotFound):
            list(
                self.client.read_stream_events(
                    stream_name, stream_position=1, backwards=True
                )
            )

        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name, limit=10))

        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name, backwards=True, limit=10))

        with self.assertRaises(StreamNotFound):
            list(
                self.client.read_stream_events(stream_name, stream_position=1, limit=10)
            )

        with self.assertRaises(StreamNotFound):
            list(
                self.client.read_stream_events(
                    stream_name, stream_position=1, backwards=True, limit=10
                )
            )

    def test_append_event_and_read_stream_with_occ(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Check stream position is None.
        self.assertEqual(self.client.get_stream_position(stream_name), None)

        # Todo: Reintroduce this when/if testing for streaming individual events.
        # # Check get error when attempting to append empty list to position 1.
        # with self.assertRaises(ExpectedPositionError) as cm:
        #     self.client.append_events(stream_name, expected_position=1, events=[])
        # self.assertEqual(cm.exception.args[0], f"Stream {stream_name!r} does not exist")

        # # Append empty list of events.
        # commit_position0 = self.client.append_events(
        #     stream_name, expected_position=None, events=[]
        # )
        # self.assertIsInstance(commit_position0, int)

        # # Check stream still not found.
        # with self.assertRaises(StreamNotFound):
        #     list(self.client.read_stream_events(stream_name))

        # # Check stream position is None.
        # self.assertEqual(self.client.get_stream_position(stream_name), None)

        # Construct three new events.
        data1 = random_data()
        event1 = NewEvent(type="OrderCreated", data=data1)

        metadata2 = random_data()
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=metadata2)

        id3 = uuid4()
        event3 = NewEvent(
            type="OrderDeleted",
            data=random_data(),
            metadata=random_data(),
            content_type="application/octet-stream",
            id=id3,
        )

        # Check the attributes of the new events.
        # Todo: Extract TestNewEvent class.
        self.assertEqual(event1.type, "OrderCreated")
        self.assertEqual(event1.data, data1)
        self.assertEqual(event1.metadata, b"")
        self.assertEqual(event1.content_type, "application/json")
        self.assertIsInstance(event1.id, UUID)

        self.assertEqual(event2.metadata, metadata2)

        self.assertEqual(event3.content_type, "application/octet-stream")
        self.assertEqual(event3.id, id3)

        # Check get error when attempting to append new event to position 1.
        with self.assertRaises(WrongExpectedPosition) as cm:
            self.client.append_event(stream_name, expected_position=1, event=event1)
        self.assertEqual(cm.exception.args[0], f"Stream {stream_name!r} does not exist")

        # Append new event.
        commit_position0 = self.client.get_commit_position()
        commit_position1 = self.client.append_event(
            stream_name, expected_position=None, event=event1
        )

        # Check commit position is greater.
        self.assertGreater(commit_position1, commit_position0)

        # Check stream position is 0.
        self.assertEqual(self.client.get_stream_position(stream_name), 0)

        # Read the stream forwards from the start (expect one event).
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 1)

        # Check the attributes of the recorded event.
        self.assertEqual(events[0].type, event1.type)
        self.assertEqual(events[0].data, event1.data)
        self.assertEqual(events[0].metadata, event1.metadata)
        self.assertEqual(events[0].content_type, event1.content_type)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[0].stream_name, stream_name)
        self.assertEqual(events[0].stream_position, 0)
        if events[0].commit_position is not None:  # v21.20 doesn't return this
            self.assertEqual(events[0].commit_position, commit_position1)

        # Check we can't append another new event at initial position.

        with self.assertRaises(WrongExpectedPosition) as cm:
            self.client.append_event(stream_name, expected_position=None, event=event2)
        self.assertEqual(cm.exception.args[0], "Current position is 0")

        # Append another event.
        commit_position2 = self.client.append_event(
            stream_name, expected_position=0, event=event2
        )

        # Check stream position is 1.
        self.assertEqual(self.client.get_stream_position(stream_name), 1)

        # Check stream position.
        self.assertGreater(commit_position2, commit_position1)

        # Read the stream (expect two events in 'forwards' order).
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

        # Read the stream backwards from the end.
        events = list(self.client.read_stream_events(stream_name, backwards=True))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event1.id)

        # Read the stream forwards from position 1.
        events = list(self.client.read_stream_events(stream_name, stream_position=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Read the stream backwards from position 0.
        events = list(
            self.client.read_stream_events(
                stream_name, stream_position=0, backwards=True
            )
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Read the stream forwards from start with limit.
        events = list(self.client.read_stream_events(stream_name, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Read the stream backwards from end with limit.
        events = list(
            self.client.read_stream_events(stream_name, backwards=True, limit=1)
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Check we can't append another new event at second position.
        with self.assertRaises(WrongExpectedPosition) as cm:
            self.client.append_event(stream_name, expected_position=0, event=event3)
        self.assertEqual(cm.exception.args[0], "Current position is 1")

        # Append another new event.
        commit_position3 = self.client.append_event(
            stream_name, expected_position=1, event=event3
        )

        # Check stream position is 2.
        self.assertEqual(self.client.get_stream_position(stream_name), 2)

        # Check the commit position.
        self.assertGreater(commit_position3, commit_position2)

        # Read the stream forwards from start (expect three events).
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Read the stream backwards from end (expect three events).
        events = list(self.client.read_stream_events(stream_name, backwards=True))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event1.id)

        # Read the stream forwards from position 1 with limit 1.
        events = list(
            self.client.read_stream_events(stream_name, stream_position=1, limit=1)
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Read the stream backwards from position 1 with limit 1.
        events = list(
            self.client.read_stream_events(
                stream_name, stream_position=1, backwards=True, limit=1
            )
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Idempotent write of event2.
        commit_position2_1 = self.client.append_event(
            stream_name, expected_position=0, event=event2
        )
        self.assertEqual(commit_position2_1, commit_position2)

        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

    def test_append_event_and_read_stream_without_occ(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        event1 = NewEvent(type="Snapshot", data=random_data())

        # Append new event.
        commit_position = self.client.append_event(
            stream_name, expected_position=-1, event=event1
        )
        events = list(
            self.client.read_stream_events(stream_name, backwards=True, limit=1)
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        # Todo: Check if commit position of recorded event is really None
        #  when reading stream events in v21.10.
        if events[0].commit_position is not None:
            self.assertEqual(events[0].commit_position, commit_position)

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
    #         stream_name, expected_position=-1, events=[event1, event2]
    #     )
    #
    #     # Read stream and check recorded events.
    #     events = list(self.client.read_stream_events(stream_name))
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
    #         stream_name, expected_position=-1, events=[event3, event4]
    #     )
    #
    #     # Read stream and check recorded events.
    #     events = list(self.client.read_stream_events(stream_name))
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
    #     with self.assertRaises(StreamNotFound):
    #         self.client.append_events_multiplexed(
    #             stream_name, expected_position=1, events=[event1, event2]
    #         )
    #
    #     # Append batch of new events.
    #     commit_position2 = self.client.append_events_multiplexed(
    #         stream_name, expected_position=None, events=[event1, event2]
    #     )
    #
    #     # Read stream and check recorded events.
    #     events = list(self.client.read_stream_events(stream_name))
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
    #             stream_name, expected_position=None, events=[event3, event4]
    #         )
    #
    #     # Fail to append (wrong expected position).
    #     with self.assertRaises(ExpectedPositionError):
    #         self.client.append_events_multiplexed(
    #             stream_name, expected_position=10, events=[event3, event4]
    #         )
    #
    #     # Read stream and check recorded events.
    #     events = list(self.client.read_stream_events(stream_name))
    #     self.assertEqual(len(events), 2)
    #     self.assertEqual(events[0].id, event1.id)
    #     self.assertEqual(events[1].id, event2.id)

    def test_append_events_with_occ(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Fail to append (stream does not exist).
        with self.assertRaises(StreamNotFound):
            self.client.append_events(
                stream_name, expected_position=1, events=[event1, event2]
            )

        # Append batch of new events.
        commit_position2 = self.client.append_events(
            stream_name, expected_position=None, events=[event1, event2]
        )

        # Read stream and check recorded events.
        events = list(self.client.read_stream_events(stream_name))
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
        with self.assertRaises(WrongExpectedPosition):
            self.client.append_events(
                stream_name, expected_position=None, events=[event3, event4]
            )

        # Fail to append (wrong expected position).
        with self.assertRaises(WrongExpectedPosition):
            self.client.append_events(
                stream_name, expected_position=10, events=[event3, event4]
            )

        # Read stream and check recorded events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

    def test_append_events_without_occ(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append batch of new events.
        commit_position2 = self.client.append_events(
            stream_name, expected_position=-1, events=[event1, event2]
        )

        # Read stream and check recorded events.
        events = list(self.client.read_stream_events(stream_name))
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
            stream_name, expected_position=-1, events=[event3, event4]
        )

        # Read stream and check recorded events.
        events = list(self.client.read_stream_events(stream_name))
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

    def test_commit_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        event1 = NewEvent(type="Snapshot", data=b"{}", metadata=b"{}")

        # Append new event.
        commit_position = self.client.append_events(
            stream_name, expected_position=-1, events=[event1]
        )
        # Check we actually have an int.
        self.assertIsInstance(commit_position, int)

        # Check commit_position() returns expected value.
        self.assertEqual(self.client.get_commit_position(), commit_position)

        # Create persistent subscription.
        self.client.create_subscription(f"group-{uuid4()}")

        # Check commit_position() still returns expected value.
        self.assertEqual(self.client.get_commit_position(), commit_position)

    def test_timeout_append_events(self) -> None:
        self.construct_esdb_client()

        # Append two events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(
            type="OrderCreated",
            data=b"{}",
            metadata=b"{}",
        )
        new_events = [event1]
        new_events += [NewEvent(type="OrderUpdated", data=b"") for _ in range(1000)]
        # Timeout appending new event.
        with self.assertRaises(DeadlineExceeded):
            self.client.append_events(
                stream_name=stream_name1,
                expected_position=None,
                events=new_events,
                timeout=0,
            )

        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name1))

        # # Timeout appending new event.
        # with self.assertRaises(DeadlineExceeded):
        #     self.client.append_events(
        #         stream_name1, expected_position=1, events=[event3], timeout=0
        #     )
        #
        # # Timeout reading stream.
        # with self.assertRaises(DeadlineExceeded):
        #     list(self.client.read_stream_events(stream_name1, timeout=0))

    def test_read_all_events_filter_default(self) -> None:
        self.construct_esdb_client()

        num_old_events = len(list(self.client.read_all_events()))

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position1 = self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        commit_position2 = self.client.append_events(
            stream_name2, expected_position=None, events=[event1, event2, event3]
        )

        # Check we can read forwards from the start.
        events = list(self.client.read_all_events())
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
        events = list(self.client.read_all_events(backwards=True))
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
        events = list(self.client.read_all_events(commit_position=commit_position1))
        self.assertEqual(len(events), 4)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderUpdated")
        self.assertEqual(events[3].stream_name, stream_name2)
        self.assertEqual(events[3].type, "OrderDeleted")

        # Check we can read forwards from commit position 2.
        events = list(self.client.read_all_events(commit_position=commit_position2))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")

        # Check we can read backwards from commit position 1.
        # NB backwards here doesn't include event at commit position, otherwise
        # first event would an OrderDeleted event, and we get an OrderUpdated.
        events = list(
            self.client.read_all_events(
                commit_position=commit_position1, backwards=True
            )
        )
        self.assertEqual(len(events) - num_old_events, 2)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name1)
        self.assertEqual(events[1].type, "OrderCreated")

        # Check we can read backwards from commit position 2.
        # NB backwards here doesn't include event at commit position.
        events = list(
            self.client.read_all_events(
                commit_position=commit_position2, backwards=True
            )
        )
        self.assertEqual(len(events) - num_old_events, 5)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name1)
        self.assertEqual(events[2].type, "OrderDeleted")

        # Check we can read forwards from the start with limit.
        events = list(self.client.read_all_events(limit=3))
        self.assertEqual(len(events), 3)

        # Check we can read backwards from the end with limit.
        events = list(self.client.read_all_events(backwards=True, limit=3))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderCreated")

        # Check we can read forwards from commit position 1 with limit.
        events = list(
            self.client.read_all_events(commit_position=commit_position1, limit=3)
        )
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderUpdated")

        # Check we can read backwards from commit position 2 with limit.
        events = list(
            self.client.read_all_events(
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

    def test_read_all_events_timeout(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        self.client.append_events(
            stream_name2, expected_position=None, events=[event1, event2, event3]
        )

        # Timeout reading all events.
        with self.assertRaises(DeadlineExceeded):
            list(self.client.read_all_events(timeout=0.001))

    def test_read_all_events_filter_include_event_types(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read only OrderCreated.
        events = list(self.client.read_all_events(filter_include=("OrderCreated",)))
        types = set([e.type for e in events])
        self.assertEqual(types, {"OrderCreated"})

        # Read only OrderCreated and OrderDeleted.
        events = list(
            self.client.read_all_events(filter_include=("OrderCreated", "OrderDeleted"))
        )
        types = set([e.type for e in events])
        self.assertEqual(types, {"OrderCreated", "OrderDeleted"})

    def test_read_all_events_filter_include_stream_identifiers(self) -> None:
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
        self.client.append_events(stream_name1, expected_position=None, events=[event1])
        self.client.append_events(stream_name2, expected_position=None, events=[event2])
        self.client.append_events(stream_name3, expected_position=None, events=[event3])

        # Read only stream1 and stream2.
        events = list(
            self.client.read_all_events(
                filter_include=(stream_name1, stream_name2), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids, {event1.id, event2.id})

        # Read only stream2 and stream3.
        events = list(
            self.client.read_all_events(
                filter_include=(stream_name2, stream_name3), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids, {event2.id, event3.id})

        # Read only prefix1.
        events = list(
            self.client.read_all_events(
                filter_include=(prefix1 + ".*",), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids, {event1.id, event2.id})

        # Read only prefix2.
        events = list(
            self.client.read_all_events(
                filter_include=(prefix2 + ".*",), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids, {event3.id})

    def test_read_all_events_filter_exclude_event_types(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Exclude OrderCreated.
        events = list(self.client.read_all_events(filter_exclude=("OrderCreated",)))
        types = set([e.type for e in events])
        self.assertNotIn("OrderCreated", types)
        self.assertIn("OrderUpdated", types)
        self.assertIn("OrderDeleted", types)

        # Exclude OrderCreated and OrderDeleted.
        events = list(
            self.client.read_all_events(filter_exclude=("OrderCreated", "OrderDeleted"))
        )
        types = set([e.type for e in events])
        self.assertNotIn("OrderCreated", types)
        self.assertIn("OrderUpdated", types)
        self.assertNotIn("OrderDeleted", types)

    def test_read_all_events_filter_exclude_stream_identifiers(self) -> None:
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
        self.client.append_events(stream_name1, expected_position=None, events=[event1])
        self.client.append_events(stream_name2, expected_position=None, events=[event2])
        self.client.append_events(stream_name3, expected_position=None, events=[event3])

        # Read everything except stream1 and stream2.
        events = list(
            self.client.read_all_events(
                filter_exclude=(stream_name1, stream_name2), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids.intersection({event1.id, event2.id}), set())

        # Read everything except stream2 and stream3.
        events = list(
            self.client.read_all_events(
                filter_exclude=(stream_name2, stream_name3), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids.intersection({event2.id, event3.id}), set())

        # Read everything except prefix1.
        events = list(
            self.client.read_all_events(
                filter_exclude=(prefix1 + ".*",), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids.intersection({event1.id, event2.id}), set())

        # Read everything except prefix2.
        events = list(
            self.client.read_all_events(
                filter_exclude=(prefix2 + ".*",), filter_by_stream_name=True
            )
        )
        event_ids = set([e.id for e in events])
        self.assertEqual(event_ids.intersection({event3.id}), set())

    def test_read_all_events_filter_include_ignores_filter_exclude(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Both include and exclude.
        events = list(
            self.client.read_all_events(
                filter_include=("OrderCreated",), filter_exclude=("OrderCreated",)
            )
        )
        types = set([e.type for e in events])
        self.assertIn("OrderCreated", types)
        self.assertNotIn("OrderUpdated", types)
        self.assertNotIn("OrderDeleted", types)

    def test_delete_stream_with_expected_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())
        event4 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        # Can't delete the stream when specifying incorrect expected position.
        with self.assertRaises(WrongExpectedPosition):
            self.client.delete_stream(stream_name, expected_position=0)

        # Delete the stream, specifying correct expected position.
        self.client.delete_stream(stream_name, expected_position=1)

        # Can't call delete again with incorrect expected position.
        with self.assertRaises(WrongExpectedPosition):
            self.client.delete_stream(stream_name, expected_position=0)

        # Can call delete again, with correct expected position.
        self.client.delete_stream(stream_name, expected_position=1)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Expect stream position is None.
        # Todo: Then how to you know which expected_position to use
        #  when subsequently appending?
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can append to a deleted stream.
        self.client.append_events(stream_name, expected_position=None, events=[event3])
        # with self.assertRaises(ExpectedPositionError):
        #     self.client.append_events(stream_name, expected_position=None, events=[event3])
        # self.client.append_events(stream_name, expected_position=1, events=[event3])

        self.assertEqual(2, self.client.get_stream_position(stream_name))
        self.client.append_events(stream_name, expected_position=2, events=[event4])

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get StreamNotFound.
        sleep(0.1)
        events = list(self.client.read_stream_events(stream_name))
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event4.id)

        # Can't delete the stream again with incorrect expected position.
        with self.assertRaises(WrongExpectedPosition):
            self.client.delete_stream(stream_name, expected_position=0)

        # Can still read the events.
        self.assertEqual(3, self.client.get_stream_position(stream_name))
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Can delete the stream again, using correct expected position.
        self.client.delete_stream(stream_name, expected_position=3)

        # Stream is now "not found".
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can't call delete again with incorrect expected position.
        with self.assertRaises(WrongExpectedPosition):
            self.client.delete_stream(stream_name, expected_position=2)

        # Can delete again without error.
        self.client.delete_stream(stream_name, expected_position=3)

    def test_delete_stream_with_any_expected_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Can't delete stream that doesn't exist, while expecting "any" version.
        # Todo: I don't fully understand why this should cause an error.
        with self.assertRaises(StreamNotFound):
            self.client.delete_stream(stream_name, expected_position=-1)

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        # Delete the stream, specifying "any" expected position.
        self.client.delete_stream(stream_name, expected_position=-1)

        # Can call delete again, without error.
        self.client.delete_stream(stream_name, expected_position=-1)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Expect stream position is None.
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can append to a deleted stream.
        with self.assertRaises(WrongExpectedPosition):
            self.client.append_events(stream_name, expected_position=0, events=[event3])
        self.client.append_events(stream_name, expected_position=1, events=[event3])

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get StreamNotFound.
        sleep(0.1)
        events = list(self.client.read_stream_events(stream_name))
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event3.id)

        # Delete the stream again, specifying "any" expected position.
        self.client.delete_stream(stream_name, expected_position=-1)
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can delete again without error.
        self.client.delete_stream(stream_name, expected_position=-1)

    def test_delete_stream_expecting_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Can't delete stream, expecting stream exists, because stream never existed.
        with self.assertRaises(StreamNotFound):
            self.client.delete_stream(stream_name, expected_position=None)

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        # Delete the stream, specifying "any" expected position.
        self.client.delete_stream(stream_name, expected_position=None)

        # Can't delete deleted stream, expecting stream exists, because it was deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.delete_stream(stream_name, expected_position=None)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Expect stream position is None.
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can't append to a deleted stream with incorrect expected position.
        with self.assertRaises(WrongExpectedPosition):
            self.client.append_events(stream_name, expected_position=0, events=[event3])

        # Can append to a deleted stream with correct expected position.
        self.client.append_events(stream_name, expected_position=None, events=[event3])

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get StreamNotFound.
        sleep(0.1)
        events = list(self.client.read_stream_events(stream_name))
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event3.id)

        # Can delete the appended stream, whilst expecting stream exists.
        self.client.delete_stream(stream_name, expected_position=None)
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can't call delete again, expecting stream exists, because it was deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.delete_stream(stream_name, expected_position=None)

    def test_tombstone_stream_with_expected_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Construct three events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        with self.assertRaises(WrongExpectedPosition):
            self.client.tombstone_stream(stream_name, expected_position=0)

        # Tombstone the stream, specifying expected position.
        self.client.tombstone_stream(stream_name, expected_position=1)

        # Can't tombstone again with correct expected position, stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name, expected_position=1)

        # Can't tombstone again with incorrect expected position, stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name, expected_position=2)

        # Can't read from stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            list(self.client.read_stream_events(stream_name))

        # Can't get stream position, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_position(stream_name)

        # Can't append to tombstoned stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.append_events(stream_name, expected_position=1, events=[event3])

    def test_tombstone_stream_with_any_expected_position(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Can tombstone stream that doesn't exist, while expecting "any" version.
        # Todo: I don't really understand why this shouldn't cause an error,
        #  if we can do this with the delete operation.
        self.client.tombstone_stream(stream_name1, expected_position=-1)

        # Construct two events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Can't append to tombstoned stream that never existed.
        with self.assertRaises(StreamIsDeleted):
            self.client.append_events(
                stream_name1, expected_position=None, events=[event1]
            )

        # Append two events to a different stream.
        stream_name2 = str(uuid4())
        self.client.append_events(stream_name2, expected_position=None, events=[event1])
        self.client.append_events(stream_name2, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name2))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name2))

        # Tombstone the stream, specifying "any" expected position.
        self.client.tombstone_stream(stream_name2, expected_position=-1)

        # Can't call tombstone again.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name2, expected_position=-1)

        with self.assertRaises(StreamIsDeleted):
            list(self.client.read_stream_events(stream_name2))

        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_position(stream_name2)

    def test_tombstone_stream_expecting_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Can't tombstone stream that doesn't exist, while expecting "stream exists".
        with self.assertRaises(StreamNotFound):
            self.client.tombstone_stream(stream_name, expected_position=None)

        # Construct two events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events.
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        # Tombstone the stream, expecting "stream exists".
        self.client.tombstone_stream(stream_name, expected_position=None)

        # Can't call tombstone again.
        with self.assertRaises(StreamIsDeleted):
            self.client.tombstone_stream(stream_name, expected_position=None)

        with self.assertRaises(StreamIsDeleted):
            list(self.client.read_stream_events(stream_name))

        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_position(stream_name)

    def test_subscribe_all_events_default_filter(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe to all events, from the start.
        subscription = self.client.subscribe_all_events()
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
            stream_name2, expected_position=None, events=[event4, event5, event6]
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

    def test_subscribe_stream_events_default_filter(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe to stream events, from the start.
        subscription = self.client.subscribe_stream_events(stream_name=stream_name1)
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
            stream_name2, expected_position=None, events=[event4, event5, event6]
        )

        # Append three more events to stream1.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=2, events=[event7, event8, event9]
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

    def test_subscribe_stream_events_from_stream_position(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())

        # Append new events.
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe to stream events, from the current stream position.
        subscription = self.client.subscribe_stream_events(
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
            stream_name2, expected_position=None, events=[event4, event5, event6]
        )

        # Append three more events to stream1.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=2, events=[event7, event8, event9]
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

    def test_subscribe_all_events_no_filter(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the current commit position.
        subscription = self.client.subscribe_all_events(
            filter_exclude=[],
            filter_include=[],
        )

        # Expect to get system events.
        for event in subscription:
            if event.type.startswith("$"):
                break
        else:
            self.fail("Didn't get a system event")

    def test_subscribe_all_events_filter_include_event_types(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the beginning.
        subscription = self.client.subscribe_all_events(
            filter_include=["OrderCreated"],
        )

        # Expect to only get "OrderCreated" events.
        events = []
        for event in subscription:
            if not event.type.startswith("OrderCreated"):
                self.fail("Event type is not 'OrderCreated'")

            events.append(event)

            # Break if we see the 'OrderCreated' event appended above.
            if event.data == event1.data:
                break

        # Check we actually got some 'OrderCreated' events.
        self.assertGreater(len(events), 0)

    def test_subscribe_all_events_filter_include_stream_names(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        stream_name2 = str(uuid4())
        stream_name3 = str(uuid4())
        self.client.append_events(stream_name1, expected_position=None, events=[event1])
        self.client.append_events(stream_name2, expected_position=None, events=[event2])
        self.client.append_events(stream_name3, expected_position=None, events=[event3])

        # Expect to only get stream_name1 events.
        subscription = self.client.subscribe_all_events(
            filter_include=stream_name1, filter_by_stream_name=True
        )
        for event in subscription:
            if event.stream_name != stream_name1:
                self.fail("Filtering included other stream names")
            if event.id == event1.id:
                break

        # Expect to only get stream_name2 events.
        subscription = self.client.subscribe_all_events(
            filter_include=stream_name2, filter_by_stream_name=True
        )
        for event in subscription:
            if event.stream_name != stream_name2:
                self.fail("Filtering included other stream names")
            if event.id == event2.id:
                break

        # Expect to only get stream_name3 events.
        subscription = self.client.subscribe_all_events(
            filter_include=stream_name3, filter_by_stream_name=True
        )
        for event in subscription:
            if event.stream_name != stream_name3:
                self.fail("Filtering included other stream names")
            if event.id == event3.id:
                break

    def test_subscribe_all_events_from_commit_position_zero(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the beginning.
        subscription = self.client.subscribe_all_events()

        # Expect to only get "OrderCreated" events.
        count = 0
        for _ in subscription:
            count += 1
            break
        self.assertEqual(count, 1)

    def test_subscribe_all_events_from_commit_position_current(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        stream_name1 = str(uuid4())
        commit_position = self.client.append_events(
            stream_name1, expected_position=None, events=[event1]
        )
        self.client.append_events(
            stream_name1, expected_position=0, events=[event2, event3]
        )

        # Subscribe from the commit position.
        subscription = self.client.subscribe_all_events(commit_position=commit_position)

        events = []
        for event in subscription:
            # Expect catch-up subscription results are exclusive of given
            # commit position, so that we expect event1 to be not included.
            if event.id == event1.id:
                self.fail("Not exclusive")

            # Collect events.
            events.append(event)

            # Break if we got the last one we wrote.
            if event.data == event3.data:
                break

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event3.id)

    def test_timeout_subscribe_all_events(self) -> None:
        self.construct_esdb_client()

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the beginning.
        subscription = self.client.subscribe_all_events(timeout=0.5)

        # Expect to timeout instead of waiting indefinitely for next event.
        count = 0
        with self.assertRaises(DeadlineExceeded):
            for _ in subscription:
                count += 1
        self.assertGreater(count, 0)

    def test_create_and_read_subscription_from_start_with_ack(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(group_name=group_name)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read all events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.ack(event.id)
            events.append(event)
            if event.data == event3.data:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_create_and_read_subscription_from_start_with_nack_unknown(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(group_name=group_name)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read all events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.nack(event.id, action="unknown")
            events.append(event)
            if event.data == event3.data:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_create_and_read_subscription_from_start_with_nack_park(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(group_name=group_name)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read all events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.nack(event.id, action="park")
            events.append(event)
            if event.data == event3.data:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_create_and_read_subscription_from_start_with_nack_retry(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(group_name=group_name)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read all events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.nack(event.id, action="retry")
            events.append(event)
            if event.data == event3.data:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

        # Should get the events again.
        events = []
        expected_event_ids = {event1.id, event2.id, event3.id}
        for event in read_resp:
            read_req.ack(event.id)
            if event.id in expected_event_ids:
                expected_event_ids.remove(event.id)
            if len(expected_event_ids) == 0:
                break

    def test_create_and_read_subscription_from_start_with_nack_skip(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(group_name=group_name)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read all events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.nack(event.id, action="skip")
            events.append(event)
            if event.data == event3.data:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_create_and_read_subscription_from_start_with_nack_stop(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(group_name=group_name)

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read all events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.nack(event.id, action="stop")
            events.append(event)
            if event.data == event3.data:
                break

        assert events[-3].data == event1.data
        assert events[-2].data == event2.data
        assert events[-1].data == event3.data

    def test_create_and_read_subscription_from_commit_position(self) -> None:
        self.construct_esdb_client()

        # Append one event.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        commit_position = self.client.append_events(
            stream_name1, expected_position=None, events=[event1]
        )

        # Append two more events.
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=0, events=[event2, event3]
        )

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"

        self.client.create_subscription(
            group_name=group_name,
            commit_position=commit_position,
        )

        # Read events from subscription.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.ack(event.id)

            events.append(event)

            if event.id == event3.id:
                break

        # Expect persistent subscription results are inclusive of given
        # commit position, so that we expect event1 to be included.

        assert events[0].id == event1.id
        assert events[1].id == event2.id
        assert events[2].id == event3.id

    def test_create_and_read_subscription_from_end(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
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
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read three events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        events = []
        for event in read_resp:
            read_req.ack(event.id)
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

    def test_create_and_read_subscription_filter_exclude_event_types(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name,
            filter_exclude=["OrderCreated"],
        )

        # Append three events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read events from subscription.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        # Check we don't receive any OrderCreated events.
        for event in read_resp:
            read_req.ack(event.id)
            self.assertNotEqual(event.type, "OrderCreated")
            if event.data == event3.data:
                break

    def test_create_and_read_subscription_filter_exclude_stream_names(self) -> None:
        self.construct_esdb_client()

        stream_name1 = str(uuid4())
        prefix1 = str(uuid4())
        stream_name2 = prefix1 + str(uuid4())
        stream_name3 = prefix1 + str(uuid4())
        stream_name4 = str(uuid4())

        # Create persistent subscriptions.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name1,
            filter_exclude=stream_name1,
            filter_by_stream_name=True,
        )
        group_name2 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name2,
            filter_exclude=prefix1 + ".*",
            filter_by_stream_name=True,
        )

        # Append events.
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
        event4 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(stream_name1, expected_position=None, events=[event1])
        self.client.append_events(stream_name2, expected_position=None, events=[event2])
        self.client.append_events(stream_name3, expected_position=None, events=[event3])
        self.client.append_events(stream_name4, expected_position=None, events=[event4])

        # Check we don't receive any events from stream_name1.
        read_req, read_resp = self.client.read_subscription(group_name=group_name1)

        for event in read_resp:
            if event.stream_name == stream_name1:
                self.fail("Received event from stream_name1")
            read_req.ack(event.id)
            if event.data == event4.data:
                break

        # Check we don't receive any events from stream names starting with prefix1.
        read_req, read_resp = self.client.read_subscription(group_name=group_name2)

        for event in read_resp:
            if event.stream_name.startswith(prefix1):
                self.fail("Received event with stream name starting with prefix1")
            read_req.ack(event.id)
            if event.data == event4.data:
                break

    def test_create_and_read_subscription_filter_include_event_types(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name,
            filter_include=["OrderCreated"],
        )

        # Append events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Check we only receive any OrderCreated events.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        for event in read_resp:
            read_req.ack(event.id)
            self.assertEqual(event.type, "OrderCreated")
            if event.data == event1.data:
                break

    def test_create_and_read_subscription_filter_include_stream_names(self) -> None:
        self.construct_esdb_client()

        stream_name1 = str(uuid4())
        prefix1 = str(uuid4())
        stream_name2 = str(uuid4())
        stream_name3 = prefix1 + str(uuid4())
        stream_name4 = prefix1 + str(uuid4())

        # Create persistent subscriptions.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name1,
            filter_include=stream_name4,
            filter_by_stream_name=True,
        )
        group_name2 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name2,
            filter_include=prefix1 + ".*",
            filter_by_stream_name=True,
        )

        # Append events.
        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")
        event4 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(stream_name1, expected_position=None, events=[event1])
        self.client.append_events(stream_name2, expected_position=None, events=[event2])
        self.client.append_events(stream_name3, expected_position=None, events=[event3])
        self.client.append_events(stream_name4, expected_position=None, events=[event4])

        # Check we only receive events from stream4.
        read_req, read_resp = self.client.read_subscription(group_name=group_name1)

        events = []
        for event in read_resp:
            read_req.ack(event1.id)
            events.append(event)
            if event.data == event4.data:
                break

        self.assertEqual(1, len(events))
        self.assertEqual(events[0].id, event4.id)

        # Check we only receive events with stream name starting with prefix1.
        read_req, read_resp = self.client.read_subscription(group_name=group_name2)

        events = []
        for event in read_resp:
            events.append(event)
            read_req.ack(event.id)
            if event.data == event4.data:
                break

        self.assertEqual(2, len(events))
        self.assertEqual(events[0].id, event3.id)
        self.assertEqual(events[1].id, event4.id)

    def test_create_and_read_subscription_no_filter(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name,
            filter_exclude=[],
            filter_include=[],
        )

        # Append three events.
        stream_name1 = str(uuid4())

        event1 = NewEvent(type="OrderCreated", data=random_data(), metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=random_data(), metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=random_data(), metadata=b"{}")

        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read events from subscription.
        read_req, read_resp = self.client.read_subscription(group_name=group_name)

        has_seen_system_event = False
        has_seen_persistent_config_event = False
        for event in read_resp:
            # Look for a "system" event.
            read_req.ack(event.id)
            if event.type.startswith("$"):
                has_seen_system_event = True
            elif event.type.startswith("PersistentConfig"):
                has_seen_persistent_config_event = True
            if has_seen_system_event and has_seen_persistent_config_event:
                break
            elif event.data == event3.data:
                self.fail("Expected a 'system' event and a 'PersistentConfig' event")

    def test_get_subscription_info(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(SubscriptionNotFound):
            self.client.get_subscription_info(group_name)

        # Create persistent subscription.
        self.client.create_subscription(group_name=group_name)

        info = self.client.get_subscription_info(group_name)
        self.assertEqual(info.group_name, group_name)

    def test_list_subscriptions(self) -> None:
        self.construct_esdb_client()

        subscriptions_before = self.client.list_subscriptions()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name,
            filter_exclude=[],
            filter_include=[],
        )

        subscriptions_after = self.client.list_subscriptions()
        self.assertEqual(len(subscriptions_before) + 1, len(subscriptions_after))
        group_names = [s.group_name for s in subscriptions_after]
        self.assertIn(group_name, group_names)

    def test_delete_subscription(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(SubscriptionNotFound):
            self.client.delete_subscription(group_name=group_name)

        # Create persistent subscription.
        self.client.create_subscription(
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

        with self.assertRaises(SubscriptionNotFound):
            self.client.delete_subscription(group_name=group_name)

    def test_create_and_read_stream_subscription_from_start(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, expected_position=None, events=[event4, event5, event6]
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_stream_subscription(
            group_name=group_name,
            stream_name=stream_name2,
        )

        # Read events from subscription.
        read_req, read_resp = self.client.read_stream_subscription(
            group_name=group_name,
            stream_name=stream_name2,
        )

        events = []
        for event in read_resp:
            read_req.ack(event.id)
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
            stream_name1, expected_position=2, events=[event7, event8, event9]
        )

        event10 = NewEvent(type="OrderCreated", data=random_data())
        event11 = NewEvent(type="OrderUpdated", data=random_data())
        event12 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, expected_position=2, events=[event10, event11, event12]
        )

        # Continue receiving events.
        for event in read_resp:
            read_req.ack(event.id)
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

    def test_create_and_read_stream_subscription_from_stream_position(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, expected_position=None, events=[event4, event5, event6]
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_stream_subscription(
            group_name=group_name,
            stream_name=stream_name2,
            stream_position=1,
        )

        # Read events from subscription.
        read_req, read_resp = self.client.read_stream_subscription(
            group_name=group_name,
            stream_name=stream_name2,
        )

        events = []
        for event in read_resp:
            read_req.ack(event.id)
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
            stream_name1, expected_position=2, events=[event7, event8, event9]
        )

        event10 = NewEvent(type="OrderCreated", data=random_data())
        event11 = NewEvent(type="OrderUpdated", data=random_data())
        event12 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, expected_position=2, events=[event10, event11, event12]
        )

        # Continue receiving events.
        for event in read_resp:
            read_req.ack(event.id)
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

    def test_create_and_read_stream_subscription_from_end(self) -> None:
        self.construct_esdb_client()

        # Append some events.
        stream_name1 = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        event3 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        event4 = NewEvent(type="OrderCreated", data=random_data())
        event5 = NewEvent(type="OrderUpdated", data=random_data())
        event6 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, expected_position=None, events=[event4, event5, event6]
        )

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_stream_subscription(
            group_name=group_name,
            stream_name=stream_name2,
            from_end=True,
        )

        # Read events from subscription.
        read_req, read_resp = self.client.read_stream_subscription(
            group_name=group_name,
            stream_name=stream_name2,
        )

        # Append some more events.
        event7 = NewEvent(type="OrderCreated", data=random_data())
        event8 = NewEvent(type="OrderUpdated", data=random_data())
        event9 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name1, expected_position=2, events=[event7, event8, event9]
        )

        event10 = NewEvent(type="OrderCreated", data=random_data())
        event11 = NewEvent(type="OrderUpdated", data=random_data())
        event12 = NewEvent(type="OrderDeleted", data=random_data())
        self.client.append_events(
            stream_name2, expected_position=2, events=[event10, event11, event12]
        )

        # Receive events from subscription.
        events = []
        for event in read_resp:
            read_req.ack(event.id)
            events.append(event)
            if event.id == event12.id:
                break

        # Check received events.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event10.id)
        self.assertEqual(events[1].id, event11.id)
        self.assertEqual(events[2].id, event12.id)

    def test_get_stream_subscription_info(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())
        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(SubscriptionNotFound):
            self.client.get_stream_subscription_info(
                group_name=group_name,
                stream_name=stream_name,
            )

        # Create persistent stream subscription.
        self.client.create_stream_subscription(
            group_name=group_name,
            stream_name=stream_name,
        )

        info = self.client.get_stream_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
        )
        self.assertEqual(info.group_name, group_name)

    def test_list_stream_subscriptions(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())

        subscriptions_before = self.client.list_stream_subscriptions(stream_name)
        self.assertEqual(subscriptions_before, [])

        # Create persistent stream subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_stream_subscription(
            group_name=group_name,
            stream_name=stream_name,
        )

        subscriptions_after = self.client.list_stream_subscriptions(stream_name)
        self.assertEqual(len(subscriptions_after), 1)
        self.assertEqual(subscriptions_after[0].group_name, group_name)

        # Actually, stream subscription also appears in "list_subscriptions()"?
        all_subscriptions = self.client.list_subscriptions()
        group_names = [s.group_name for s in all_subscriptions]
        self.assertIn(group_name, group_names)

    def test_delete_stream_subscription(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())
        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(SubscriptionNotFound):
            self.client.delete_stream_subscription(
                group_name=group_name, stream_name=stream_name
            )

        # Create persistent stream subscription.
        self.client.create_stream_subscription(
            group_name=group_name,
            stream_name=stream_name,
        )

        subscriptions_before = self.client.list_stream_subscriptions(stream_name)
        self.assertEqual(len(subscriptions_before), 1)

        self.client.delete_stream_subscription(
            group_name=group_name, stream_name=stream_name
        )

        subscriptions_after = self.client.list_stream_subscriptions(stream_name)
        self.assertEqual(len(subscriptions_after), 0)

        with self.assertRaises(SubscriptionNotFound):
            self.client.delete_stream_subscription(
                group_name=group_name, stream_name=stream_name
            )

    # Todo: consumer_strategy, RoundRobin and Pinned, need to test with more than
    #  one consumer, also code this as enum rather than a string
    # Todo: Nack? exception handling on callback?
    # Todo: update subscription
    # Todo: filter options
    # Todo: subscribe from end? not interesting, because you can get commit position

    def test_get_and_set_stream_metadata(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Append batch of new events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.client.append_events(
            stream_name, expected_position=None, events=[event1, event2]
        )
        self.assertEqual(2, len(list(self.client.read_stream_events(stream_name))))

        # Get stream metadata (should be empty).
        stream_metadata, position = self.client.get_stream_metadata(stream_name)
        self.assertEqual(stream_metadata, {})

        # Delete stream.
        self.client.delete_stream(stream_name, expected_position=None)
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Get stream metadata (should have "$tb").
        stream_metadata, position = self.client.get_stream_metadata(stream_name)
        self.assertIsInstance(stream_metadata, dict)
        self.assertIn("$tb", stream_metadata)
        max_long = 9223372036854775807
        self.assertEqual(stream_metadata["$tb"], max_long)

        # Set stream metadata.
        stream_metadata["foo"] = "bar"
        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=stream_metadata,
            expected_position=position,
        )

        # Check the metadata has "foo".
        stream_metadata, position = self.client.get_stream_metadata(stream_name)
        self.assertEqual(stream_metadata["foo"], "bar")

        # For some reason "$tb" is now (most often) 2 rather than max_long.
        # Todo: Why is this?
        self.assertIn(stream_metadata["$tb"], [2, max_long])

        # Get and set metadata for a stream that does not exist.
        stream_name = str(uuid4())
        stream_metadata, position = self.client.get_stream_metadata(stream_name)
        self.assertEqual(stream_metadata, {})

        stream_metadata["foo"] = "baz"
        self.client.set_stream_metadata(stream_name, stream_metadata, position)
        stream_metadata, position = self.client.get_stream_metadata(stream_name)
        self.assertEqual(stream_metadata["foo"], "baz")

        # Set ACL.
        self.assertNotIn("$acl", stream_metadata)
        acl = {
            "$w": "$admins",
            "$r": "$all",
            "$d": "$admins",
            "$mw": "$admins",
            "$mr": "$admins",
        }
        stream_metadata["$acl"] = acl
        self.client.set_stream_metadata(stream_name, stream_metadata, position)
        stream_metadata, position = self.client.get_stream_metadata(stream_name)
        self.assertEqual(stream_metadata["$acl"], acl)

        with self.assertRaises(WrongExpectedPosition):
            self.client.set_stream_metadata(
                stream_name=stream_name,
                metadata=stream_metadata,
                expected_position=10,
            )

        self.client.tombstone_stream(stream_name, expected_position=-1)

        # Can't get metadata after tombstoning stream, because stream is deleted.
        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_metadata(stream_name)

        # For some reason, we can set stream metadata, even though the stream
        # has been tombstoned, and even though we can't get stream metadata.
        # Todo: Ask ESDB team why this is?
        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=stream_metadata,
            expected_position=1,
        )

        self.client.set_stream_metadata(
            stream_name=stream_name,
            metadata=stream_metadata,
            expected_position=-1,
        )

        with self.assertRaises(StreamIsDeleted):
            self.client.get_stream_metadata(stream_name)

    def test_read_gossip(self) -> None:
        self.construct_esdb_client()
        cluster_info = self.client.read_gossip()
        if self.ESDB_CLUSTER_SIZE == 1:
            self.assertEqual(len(cluster_info), 1)
            self.assertEqual(cluster_info[0].state, NODE_STATE_LEADER)
            self.assertEqual(cluster_info[0].address, "localhost")
            self.assertIn(str(cluster_info[0].port), self.ESDB_TARGET)
        elif self.ESDB_CLUSTER_SIZE == 3:
            self.assertEqual(len(cluster_info), 3)
            num_leaders = 0
            num_followers = 0
            for member_info in cluster_info:
                if member_info.state == NODE_STATE_LEADER:
                    num_leaders += 1
                elif member_info.state == NODE_STATE_FOLLOWER:
                    num_followers += 1
            self.assertEqual(num_leaders, 1)
            self.assertEqual(num_followers, 2)
        else:
            self.fail(f"Test doesn't work with cluster size {self.ESDB_CLUSTER_SIZE}")

    def test_read_cluster_gossip(self) -> None:
        self.construct_esdb_client()
        cluster_info = self.client.read_cluster_gossip()
        if self.ESDB_CLUSTER_SIZE == 1:
            self.assertEqual(len(cluster_info), 1)
            self.assertEqual(cluster_info[0].state, NODE_STATE_LEADER)
            self.assertEqual(cluster_info[0].address, "127.0.0.1")
            self.assertEqual(cluster_info[0].port, 2113)
        elif self.ESDB_CLUSTER_SIZE == 3:
            self.assertEqual(len(cluster_info), 3)
            num_leaders = 0
            num_followers = 0
            for member_info in cluster_info:
                if member_info.state == NODE_STATE_LEADER:
                    num_leaders += 1
                elif member_info.state == NODE_STATE_FOLLOWER:
                    num_followers += 1
            self.assertEqual(num_leaders, 1)
            self.assertEqual(num_followers, 2)
        else:
            self.fail(f"Test doesn't work with cluster size {self.ESDB_CLUSTER_SIZE}")


class TestESDBClientWithInsecureConnection(TestESDBClient):
    ESDB_TARGET = "localhost:2114"
    ESDB_TLS = False

    def test_raises_service_unavailable_exception(self) -> None:
        # Getting DeadlineExceeded as __cause__ with "insecure" server.
        pass


class TestESDBClusterNode1(TestESDBClient):
    ESDB_TARGET = "127.0.0.1:2111"
    ESDB_CLUSTER_SIZE = 3


class TestESDBClusterNode2(TestESDBClient):
    ESDB_TARGET = "127.0.0.1:2112"
    ESDB_CLUSTER_SIZE = 3


class TestESDBClusterNode3(TestESDBClient):
    ESDB_TARGET = "127.0.0.1:2113"
    ESDB_CLUSTER_SIZE = 3


class TestESDBDiscoverScheme(TestCase):
    def test(self) -> None:
        uri = (
            "esdb+discover://example.com"
            "?Tls=false&GossipTimeout=0&DiscoveryInterval=0&MaxDiscoverAttempts=2"
        )
        with self.assertRaises(DiscoveryFailed):
            ESDBClient(uri)


class TestGrpcOptions(TestCase):
    def setUp(self) -> None:
        uri = (
            "esdb://localhost:2114"
            "?Tls=false&KeepAliveInterval=1000&KeepAliveTimeout=1000"
        )
        self.client = ESDBClient(uri)

    def tearDown(self) -> None:
        self.client.close()

    def test(self) -> None:
        self.assertEqual(
            self.client.grpc_options["grpc.max_receive_message_length"],
            17 * 1024 * 1024,
        )
        self.assertEqual(self.client.grpc_options["grpc.keepalive_ms"], 1000)
        self.assertEqual(self.client.grpc_options["grpc.keepalive_timeout_ms"], 1000)


class TestSeparateReadAndWriteClients(TestCase):
    def setUp(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2111"
        ca_cert = read_ca_cert()
        self.writer = ESDBClient(
            uri + "?NodePreference=leader", root_certificates=ca_cert
        )
        self.reader = ESDBClient(
            uri + "?NodePreference=follower", root_certificates=ca_cert
        )

    def tearDown(self) -> None:
        self.writer.close()
        self.reader.close()

    def test_can_write_to_leader_and_read_from_follower(self) -> None:
        # Write to leader.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.writer.append_events(
            stream_name=stream_name,
            expected_position=None,
            events=[event1, event2],
        )
        # Read from follower.
        recorded_events = []
        for recorded_event in self.reader.subscribe_stream_events(
            stream_name, timeout=5
        ):
            recorded_events.append(recorded_event)
            if len(recorded_events) == 2:
                break

    def test_cannot_write_to_follower(self) -> None:
        # Try writing to follower...
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        with self.assertRaises(NodeIsNotLeader):
            self.reader.append_event(stream_name, expected_position=None, event=event1)

        # Todo: Sometimes this just causes an "Exception was thrown by handler." error.
        with self.assertRaises(NodeIsNotLeader):
            self.reader.append_events(
                stream_name, expected_position=None, events=[event1, event2]
            )

        with self.assertRaises(NodeIsNotLeader):
            self.reader.set_stream_metadata(stream_name, metadata={})

        with self.assertRaises(NodeIsNotLeader):
            self.reader.delete_stream(stream_name, expected_position=None)

        with self.assertRaises(NodeIsNotLeader):
            self.reader.tombstone_stream(stream_name, expected_position=None)

        with self.assertRaises(NodeIsNotLeader):
            self.reader.create_subscription(group_name="group1")

        with self.assertRaises(NodeIsNotLeader):
            self.reader.create_stream_subscription(
                group_name="group1", stream_name=stream_name
            )


class TestAutoReconnectToNewLeader(TestCase):
    def setUp(self) -> None:
        self.uri = "esdb://admin:changeit@127.0.0.1:2111"
        self.ca_cert = read_ca_cert()
        self.writer = ESDBClient(
            self.uri + "?NodePreference=leader", root_certificates=self.ca_cert
        )
        self.reader = ESDBClient(
            self.uri + "?NodePreference=follower", root_certificates=self.ca_cert
        )

        # Give the writer a connection to a follower.
        old, self.writer._connection = self.writer._connection, self.reader._connection

        # - this is hopeful mitigation for the "Exception was thrown by handler."
        #   occasionally issue in test_append_events()
        old.close()
        sleep(0.1)

    def tearDown(self) -> None:
        self.writer.close()
        self.reader.close()

    def test_append_event(self) -> None:
        # Append event.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())

        self.writer.append_event(stream_name, expected_position=None, event=event1)

    def test_append_events(self) -> None:
        # Append events.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Todo: Occasionally getting "Exception was thrown by handler." from this. Why?
        #   esdbclient.exceptions.ExceptionThrownByHandler: <_MultiThreadedRendezvous of
        #   RPC that terminated with:
        #       status = StatusCode.UNKNOWN
        #       details = "Exception was thrown by handler."
        #       debug_error_string = "UNKNOWN:Error received from peer  {grpc_message:"
        #       Exception was thrown by handler.", grpc_status:2, created_time:"2023-05
        #       -07T12:04:26.287327771+00:00"}"
        self.writer.append_events(
            stream_name, expected_position=None, events=[event1, event2]
        )

    def test_set_stream_metadata(self) -> None:
        self.writer.set_stream_metadata(str(uuid4()), metadata={})

    def test_delete_stream(self) -> None:
        # Setup again (need different setup)...
        self.writer.close()
        self.reader.close()

        self.writer = ESDBClient(
            self.uri + "?NodePreference=leader", root_certificates=self.ca_cert
        )
        self.reader = ESDBClient(
            self.uri + "?NodePreference=follower", root_certificates=self.ca_cert
        )

        # Need to append some events before deleting stream...
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        self.writer.append_events(
            stream_name, expected_position=None, events=[event1, event2]
        )

        # Give the writer a connection to the follower.
        self.writer._connection = self.reader._connection

        self.writer.delete_stream(stream_name, expected_position=1)

    def test_tombstone_stream(self) -> None:
        self.writer.tombstone_stream(str(uuid4()), expected_position=-1)

    def test_create_subscription(self) -> None:
        self.writer.create_subscription(group_name=f"group{str(uuid4())}")

    def test_create_stream_subscription(self) -> None:
        self.writer.create_stream_subscription(
            group_name="group1", stream_name=str(uuid4())
        )


class TestAutoReconnectToPreferredNode(TestCase):
    def setUp(self) -> None:
        self.uri = "esdb://admin:changeit@127.0.0.1:2111"
        self.ca_cert = read_ca_cert()
        self.writer = ESDBClient(
            self.uri + "?NodePreference=leader", root_certificates=self.ca_cert
        )
        self.writer.close()

    def tearDown(self) -> None:
        self.writer.close()

    def test_append_events(self) -> None:
        # Append events - should reconnect.
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        self.writer.append_events(stream_name, expected_position=None, events=[event1])

    def test_read_all_events(self) -> None:
        # Read all events - should reconnect.
        self.writer.read_all_events()

    def test_read_stream_events(self) -> None:
        # Read all events - should reconnect.
        resp = self.writer.read_stream_events(str(uuid4()))
        with self.assertRaises(StreamNotFound):
            list(resp)

    def test_read_subscription(self) -> None:
        # Read subscription - should reconnect.
        req, resp = self.writer.read_subscription(str(uuid4()))
        with self.assertRaises(SubscriptionNotFound):
            list(resp)


class TestAutoReconnectAfterServiceUnavailable(TestCase):
    def setUp(self) -> None:
        uri = "esdb://admin:changeit@localhost:2115?MaxDiscoverAttempts=1&DiscoveryInterval=0"
        server_certificate = get_server_certificate("localhost:2115")
        self.client = ESDBClient(uri=uri, root_certificates=server_certificate)

        # Reconstruct connection with wrong port (to inspire ServiceUnavailble).
        self.client._connection.close()
        self.client._connection = self.client._construct_connection("localhost:2222")

    def tearDown(self) -> None:
        self.client.close()

    def test_append_events(self) -> None:
        self.client.append_events(
            str(uuid4()), expected_position=None, events=[NewEvent(type="X", data=b"")]
        )

    def test_append_event(self) -> None:
        self.client.append_event(
            str(uuid4()), expected_position=None, event=NewEvent(type="X", data=b"")
        )
        self.client.append_event(
            str(uuid4()), expected_position=None, event=NewEvent(type="X", data=b"")
        )

    def test_create_subscription(self) -> None:
        self.client.create_subscription(group_name=f"my-subscription-{uuid4().hex}")

    def test_create_stream_subscription(self) -> None:
        self.client.create_stream_subscription(
            group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
        )

    def test_subscribe_all_events(self) -> None:
        self.client.subscribe_all_events()

    def test_subscribe_stream_events(self) -> None:
        self.client.subscribe_stream_events(str(uuid4()))

    def test_get_subscription_info(self) -> None:
        with self.assertRaises(SubscriptionNotFound):
            self.client.get_subscription_info(
                group_name=f"my-subscription-{uuid4().hex}"
            )

    def test_list_subscriptions(self) -> None:
        self.client.list_subscriptions()

    def test_list_stream_subscriptions(self) -> None:
        self.client.list_stream_subscriptions(stream_name=str(uuid4()))

    def test_delete_stream(self) -> None:
        with self.assertRaises(StreamNotFound):
            self.client.delete_stream(stream_name=str(uuid4()), expected_position=None)

    def test_delete_subscription(self) -> None:
        with self.assertRaises(SubscriptionNotFound):
            self.client.delete_subscription(group_name=f"my-subscription-{uuid4().hex}")

    def test_delete_stream_subscription(self) -> None:
        with self.assertRaises(SubscriptionNotFound):
            self.client.delete_stream_subscription(
                group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
            )

    def test_read_gossip(self) -> None:
        self.client.read_gossip()

    def test_read_cluster_gossip(self) -> None:
        self.client.read_cluster_gossip()


class TestConnectToPreferredNode(TestCase):
    def test_no_followers(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2114?Tls=false&NodePreference=follower"
        with self.assertRaises(FollowerNotFound):
            ESDBClient(uri)

    def test_no_read_only_replicas(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2114?Tls=false&NodePreference=readonlyreplica"
        with self.assertRaises(ReadOnlyReplicaNotFound):
            ESDBClient(uri)

    def test_random(self) -> None:
        uri = "esdb://admin:changeit@127.0.0.1:2114?Tls=false&NodePreference=random"
        ESDBClient(uri)


class TestSubscriptionReadRequest(TestCase):
    def test_request_ack_after_100_acks(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do one batch of acks.
        event_ids: List[UUID] = []
        for _ in range(100):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.ack(event_id)
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req.ack.ids), 100)
        self.assertEqual(len(grpc_read_req.nack.ids), 0)

        # Do another batch of acks.
        event_ids.clear()
        for _ in range(100):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.ack(event_id)
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req.ack.ids), 100)
        self.assertEqual(len(grpc_read_req.nack.ids), 0)

    def test_request_nack_after_100_nacks(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do one batch of acks.
        event_ids: List[UUID] = []
        for _ in range(100):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.nack(event_id, "park")
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(len(grpc_read_req.nack.ids), 100)

        # Do another batch of acks.
        event_ids.clear()
        for _ in range(100):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.nack(event_id, "park")
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(len(grpc_read_req.nack.ids), 100)

    def test_request_ack_after_100ms(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do three acks.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        read_request.ack(event_id2)
        read_request.ack(event_id3)
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 3)
        self.assertEqual(len(grpc_read_req.nack.ids), 0)
        self.assertEqual(grpc_read_req.ack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.ack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req.ack.ids[2].string, str(event_id3))

    def test_request_nack_unknown_after_100ms(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do three nack unknown.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "unknown")
        read_request.nack(event_id2, "unknown")
        read_request.nack(event_id3, "unknown")
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.nack.ids), 3)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.nack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req.nack.ids[2].string, str(event_id3))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Unknown)

    def test_request_nack_park_after_100ms(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        read_request.nack(event_id2, "park")
        read_request.nack(event_id3, "park")
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.nack.ids), 3)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.nack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req.nack.ids[2].string, str(event_id3))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Park)

    def test_request_nack_retry_after_100ms(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do three nack retry.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "retry")
        read_request.nack(event_id2, "retry")
        read_request.nack(event_id3, "retry")
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.nack.ids), 3)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.nack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req.nack.ids[2].string, str(event_id3))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Retry)

    def test_request_nack_skip_after_100ms(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do three nack skip.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "skip")
        read_request.nack(event_id2, "skip")
        read_request.nack(event_id3, "skip")
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.nack.ids), 3)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.nack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req.nack.ids[2].string, str(event_id3))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Skip)

    def test_request_nack_stop_after_100ms(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        # Do three nack stop.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "stop")
        read_request.nack(event_id2, "stop")
        read_request.nack(event_id3, "stop")
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.nack.ids), 3)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.nack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req.nack.ids[2].string, str(event_id3))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Stop)

    def test_request_ack_after_ack_followed_by_nack(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        read_request.ack(event_id1)

        event_id2 = uuid4()
        read_request.nack(event_id2, "park")

        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 1)
        self.assertEqual(grpc_read_req.ack.ids[0].string, str(event_id1))
        self.assertEqual(len(grpc_read_req.nack.ids), 0)

        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(len(grpc_read_req.nack.ids), 1)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id2))

    def test_request_nack_after_nack_followed_by_ack(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        read_request.nack(event_id1, "park")

        event_id2 = uuid4()
        read_request.ack(event_id2)

        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(len(grpc_read_req.nack.ids), 1)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))

        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 1)
        self.assertEqual(len(grpc_read_req.nack.ids), 0)
        self.assertEqual(grpc_read_req.ack.ids[0].string, str(event_id2))

    def test_request_nack_after_nack_followed_by_nack_with_other_action(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req.options.buffer_size, 100)

        event_id1 = uuid4()
        read_request.nack(event_id1, "park")

        event_id2 = uuid4()
        read_request.nack(event_id2, "skip")

        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(len(grpc_read_req.nack.ids), 1)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Park)

        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 0)
        self.assertEqual(len(grpc_read_req.nack.ids), 1)
        self.assertEqual(grpc_read_req.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req.nack.action, persistent_pb2.ReadReq.Nack.Skip)


class TestHandleRpcError(TestCase):
    def test_handle_exception_thrown_by_handler(self) -> None:
        with self.assertRaises(ExceptionThrownByHandler):
            raise handle_rpc_error(FakeExceptionThrownByHandlerError()) from None

    def test_handle_deadline_exceeded_error(self) -> None:
        with self.assertRaises(DeadlineExceeded):
            raise handle_rpc_error(FakeDeadlineExceededRpcError()) from None

    def test_handle_unavailable_error(self) -> None:
        with self.assertRaises(ServiceUnavailable):
            raise handle_rpc_error(FakeUnavailableRpcError()) from None

    def test_handle_writing_to_follower_error(self) -> None:
        with self.assertRaises(NodeIsNotLeader):
            raise handle_rpc_error(FakeWritingToFollowerError()) from None

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


class FakeUnknownRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.UNKNOWN)


def random_data() -> bytes:
    return os.urandom(16)
