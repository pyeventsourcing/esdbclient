from __future__ import annotations

import datetime
import json
import os
import ssl
import sys
from collections import Counter
from tempfile import NamedTemporaryFile
from threading import Thread
from time import sleep
from typing import TYPE_CHECKING, Any, cast
from unittest import TestCase, skip, skipIf
from uuid import UUID, uuid4

from grpc import RpcError, StatusCode
from grpc._channel import _MultiThreadedRendezvous, _RPCState
from grpc._cython.cygrpc import IntegratedCall

import kurrentdbclient.protos.v1.persistent_pb2 as grpc_persistent
from kurrentdbclient import (
    DEFAULT_EXCLUDE_FILTER,
    KDB_SYSTEM_EVENTS_REGEX,
    RecordedEvent,
    StreamState,
)
from kurrentdbclient.client import KurrentDBClient
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
    handle_rpc_error,
)
from kurrentdbclient.connection_spec import (
    NODE_PREFERENCE_FOLLOWER,
    NODE_PREFERENCE_LEADER,
    ConnectionSpec,
)
from kurrentdbclient.events import CaughtUp, Checkpoint, NewEvent, NewEvents
from kurrentdbclient.exceptions import (
    AbortedByServerError,
    AlreadyExistsError,
    ConsumerTooSlowError,
    DeadlineExceededError,
    DiscoveryFailedError,
    ExceptionIteratingRequestsError,
    ExceptionThrownByHandlerError,
    FailedPreconditionError,
    FollowerNotFoundError,
    GrpcDeadlineExceededError,
    GrpcError,
    InternalError,
    KurrentDBClientError,
    MaximumSubscriptionsReachedError,
    MultiAppendToSameStreamError,
    NodeIsNotLeaderError,
    NotFoundError,
    OperationFailedError,
    ProgrammingError,
    ReadOnlyReplicaNotFoundError,
    RecordMaxSizeExceededError,
    ServiceUnavailableError,
    SSLError,
    StreamIsDeletedError,
    StreamTombstonedError,
    TransactionMaxSizeExceededError,
    UnauthenticatedError,
    UnknownError,
    WrongCurrentVersionError,
)
from kurrentdbclient.gossip import NODE_STATE_FOLLOWER, NODE_STATE_LEADER
from kurrentdbclient.persistent import ConnectionInfo, SubscriptionReadReqs
from kurrentdbclient.projections import ProjectionStatistics
from kurrentdbclient.protos.v1 import persistent_pb2
from kurrentdbclient.streams import handle_streams_rpc_error
from tests.test_unpack_error_status import (
    status_with_append_record_size_exceeded_error_details_v2,
    status_with_append_transaction_size_exceeded_error_details_v2,
    status_with_not_leader_node_error_details_v2,
    status_with_some_unsupported_error_details_v2,
    status_with_stream_already_in_append_session_error_details_v2,
    status_with_stream_revision_conflict_error_details_v2,
    status_with_stream_tombstoned_error_details_v2,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from unittest.case import _AssertRaisesContext


started = datetime.datetime.now()
last = datetime.datetime.now()

KURRENTDB_DOCKER_IMAGE = os.environ.get("KURRENTDB_DOCKER_IMAGE", "25.1")

if "21.9" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (21, 9)
elif "22.10" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (22, 10)
elif "23.10" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (23, 10)
elif "24.2" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (24, 2)
elif "24.6" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (24, 6)
elif "24.10" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (24, 10)
elif "25.0" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (25, 0)
elif "25.1" in KURRENTDB_DOCKER_IMAGE:
    SERVER_VERSION = (25, 1)
else:
    msg = "Couldn't extract server version from KURRENTDB_DOCKER_IMAGE"
    raise ValueError(msg)


# os.environ["GRPC_VERBOSITY"] = "debug"
# os.environ["GRPC_TRACE"] = "all"


def get_elapsed_time() -> str:
    global last  # noqa: PLW0603
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
    def test_constructor_raises_value_errors(self) -> None:
        # Invalid scheme.
        with self.assertRaises(ValueError) as cm1:
            ConnectionSpec(uri="http://localhost:2222")
        self.assertIn("Invalid URI scheme:", cm1.exception.args[0])

        # No targets specified.
        with self.assertRaises(ValueError) as cm1:
            ConnectionSpec(uri="kdb://")
        self.assertIn("No targets specified:", cm1.exception.args[0])

        # More than one target specified.
        with self.assertRaises(ValueError) as cm1:
            ConnectionSpec(uri="kdb+discover://localhost:2222,localhost:2223")
        self.assertIn("More than one target specified:", cm1.exception.args[0])

        # Secure without username or password.
        with self.assertRaises(ValueError) as cm0:
            ConnectionSpec(uri="kdb://localhost:2222")
        self.assertIn(
            "Username and password are required",
            cm0.exception.args[0],
        )

    def test_uri(self) -> None:
        uri = "kdb://host1:2110?Tls=false"
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.uri, uri)

    def test_scheme(self) -> None:
        spec = ConnectionSpec("esdb://host1:2110?Tls=false")
        self.assertEqual(spec.scheme, "esdb")

        spec = ConnectionSpec("esdb+discover://host1:2110?Tls=false")
        self.assertEqual(spec.scheme, "esdb+discover")

        spec = ConnectionSpec("kurrentdb://host1:2110?Tls=false")
        self.assertEqual(spec.scheme, "kurrentdb")

        spec = ConnectionSpec("kurrentdb+discover://host1:2110?Tls=false")
        self.assertEqual(spec.scheme, "kurrentdb+discover")

        spec = ConnectionSpec("kdb://host1:2110?Tls=false")
        self.assertEqual(spec.scheme, "kdb")

        spec = ConnectionSpec("kdb+discover://host1:2110?Tls=false")
        self.assertEqual(spec.scheme, "kdb+discover")

    def test_targets(self) -> None:
        spec = ConnectionSpec("kdb://host1:2110?Tls=false")
        self.assertEqual(spec.targets, ["host1:2110"])

        spec = ConnectionSpec("kdb://host1:2110,host2:2111,host3:2112?Tls=false")
        self.assertEqual(spec.targets, ["host1:2110", "host2:2111", "host3:2112"])

    def test_tls(self) -> None:
        # Tls default true.
        spec = ConnectionSpec("kdb://admin:changeit@localhost:2222")
        self.assertTrue(spec.options.tls)

        # Set Tls "true".
        spec = ConnectionSpec("kdb://admin:changeit@localhost:2222?Tls=true")
        self.assertTrue(spec.options.tls)

        # Set Tls "false".
        spec = ConnectionSpec("kdb://localhost:2222?Tls=false")
        self.assertFalse(spec.options.tls)

        # Check case insensitivity.
        spec = ConnectionSpec("kdb://admin:changeit@localhost:2222?TLS=true")
        self.assertTrue(spec.options.tls)
        spec = ConnectionSpec("kdb://admin:changeit@localhost:2222?tls=true")
        self.assertTrue(spec.options.tls)
        spec = ConnectionSpec("kdb://admin:changeit@localhost:2222?tls=TRUE")
        self.assertTrue(spec.options.tls)
        spec = ConnectionSpec("kdb://localhost:2222?TLS=false")
        self.assertFalse(spec.options.tls)
        spec = ConnectionSpec("kdb://localhost:2222?tls=false")
        self.assertFalse(spec.options.tls)
        spec = ConnectionSpec("kdb://localhost:2222?tls=FALSE")
        self.assertFalse(spec.options.tls)

        # Invalid value.
        with self.assertRaises(ValueError):
            ConnectionSpec("kdb://localhost:2222?Tls=blah")

        # Repeated field (use first value).
        spec = ConnectionSpec("kdb://admin:changeit@localhost:2222?Tls=true&Tls=false")
        self.assertTrue(spec.options.tls)
        spec = ConnectionSpec("kdb://localhost:2222?Tls=false&Tls=true")
        self.assertFalse(spec.options.tls)

    def test_connection_name(self) -> None:
        # ConnectionName not mentioned.
        uri = "kdb://localhost:2222?Tls=false"
        spec = ConnectionSpec(uri)
        self.assertIsInstance(spec.options.connection_name, str)

        # Set ConnectionName.
        connection_name = str(uuid4())
        spec = ConnectionSpec(uri + f"&ConnectionName={connection_name}")
        self.assertEqual(spec.options.connection_name, connection_name)

        # Check case insensitivity.
        spec = ConnectionSpec(uri + f"&connectionName={connection_name}")
        self.assertEqual(spec.options.connection_name, connection_name)

        # Check case insensitivity.
        spec = ConnectionSpec(uri + f"&connectionName={connection_name}")
        self.assertEqual(spec.options.connection_name, connection_name)

    def test_max_discover_attempts(self) -> None:
        # MaxDiscoverAttempts not mentioned.
        uri = "kdb://localhost:2222?Tls=false"
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.max_discover_attempts, 10)

        # Set MaxDiscoverAttempts.
        spec = ConnectionSpec(uri + "&MaxDiscoverAttempts=5")
        self.assertEqual(spec.options.max_discover_attempts, 5)

    def test_discovery_interval(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"
        # DiscoveryInterval not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.discovery_interval, 100)

        # Set DiscoveryInterval.
        spec = ConnectionSpec(uri + "&DiscoveryInterval=200")
        self.assertEqual(spec.options.discovery_interval, 200)

    def test_gossip_timeout(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"
        # GossipTimeout not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.gossip_timeout, 5)

        # Set GossipTimeout.
        spec = ConnectionSpec(uri + "&GossipTimeout=10")
        self.assertEqual(spec.options.gossip_timeout, 10)

    def test_node_preference(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"
        # NodePreference not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_LEADER)

        # Set NodePreference.
        spec = ConnectionSpec(uri + "&NodePreference=leader")
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec(uri + "&NodePreference=follower")
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_FOLLOWER)

        # Invalid value.
        with self.assertRaises(ValueError):
            ConnectionSpec(uri + "&NodePreference=blah")

        # Case insensitivity.
        spec = ConnectionSpec(uri + "&nodePreference=leader")
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec(uri + "&NODEPREFERENCE=leader")
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec(uri + "&NodePreference=Leader")
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_LEADER)
        spec = ConnectionSpec(uri + "&NodePreference=FOLLOWER")
        self.assertEqual(spec.options.node_preference, NODE_PREFERENCE_FOLLOWER)

    def test_tls_verify_cert(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"
        # TlsVerifyCert not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.tls_verify_cert, True)

        # Set TlsVerifyCert.
        spec = ConnectionSpec(uri + "&TlsVerifyCert=true")
        self.assertEqual(spec.options.tls_verify_cert, True)
        spec = ConnectionSpec(uri + "&TlsVerifyCert=false")
        self.assertEqual(spec.options.tls_verify_cert, False)

        # Invalid value.
        with self.assertRaises(ValueError):
            ConnectionSpec(uri + "&TlsVerifyCert=blah")

        # Case insensitivity.
        spec = ConnectionSpec(uri + "&TLSVERIFYCERT=true")
        self.assertEqual(spec.options.tls_verify_cert, True)
        spec = ConnectionSpec(uri + "&tlsverifycert=false")
        self.assertEqual(spec.options.tls_verify_cert, False)
        spec = ConnectionSpec(uri + "&TlsVerifyCert=True")
        self.assertEqual(spec.options.tls_verify_cert, True)
        spec = ConnectionSpec(uri + "&TlsVerifyCert=False")
        self.assertEqual(spec.options.tls_verify_cert, False)

    def test_default_deadline(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        # DefaultDeadline not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.default_deadline, None)

        # Set DefaultDeadline.
        spec = ConnectionSpec(uri + "&DefaultDeadline=10")
        self.assertEqual(spec.options.default_deadline, 10)

    def test_keep_alive_interval(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        # KeepAliveInterval not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.keep_alive_interval, None)

        # Set KeepAliveInterval.
        spec = ConnectionSpec(uri + "&KeepAliveInterval=10")
        self.assertEqual(spec.options.keep_alive_interval, 10)

    def test_keep_alive_timeout(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        # KeepAliveTimeout not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.keep_alive_timeout, None)

        # Set KeepAliveTimeout.
        spec = ConnectionSpec(uri + "&KeepAliveTimeout=10")
        self.assertEqual(spec.options.keep_alive_timeout, 10)

    def test_tls_ca_file(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        # TlsCaFile not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.tls_ca_file, None)

        # Set TlsCaFile.
        spec = ConnectionSpec(uri + "&TlsCaFile=some-ca-file")
        self.assertEqual(spec.options.tls_ca_file, "some-ca-file")

    def test_user_cert_file(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        # UserCertFile not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.user_cert_file, None)

        # Set UserCertFile.
        spec = ConnectionSpec(uri + "&UserCertFile=some-path")
        self.assertEqual(spec.options.user_cert_file, "some-path")

        # Set userCertFile.
        spec = ConnectionSpec(uri + "&userCertFile=some-path")
        self.assertEqual(spec.options.user_cert_file, "some-path")

    def test_user_key_file(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        # UserKeyFile not mentioned.
        spec = ConnectionSpec(uri)
        self.assertEqual(spec.options.user_key_file, None)

        # Set UserKeyFile.
        spec = ConnectionSpec(uri + "&UserKeyFile=some-key")
        self.assertEqual(spec.options.user_key_file, "some-key")

        # Set userKeyFile.
        spec = ConnectionSpec(uri + "&userKeyFile=some-key")
        self.assertEqual(spec.options.user_key_file, "some-key")

    def test_raises_when_query_string_has_unsupported_field(self) -> None:
        uri = "kdb://localhost:2222?Tls=false"

        with self.assertRaises(ValueError) as cm1:
            ConnectionSpec(uri + "&NotSupported=10")
        self.assertIn("Unknown field in", cm1.exception.args[0])

        with self.assertRaises(ValueError) as cm2:
            ConnectionSpec(uri + "&NotSupported=10&AlsoNotSupported=20")
        self.assertIn("Unknown fields in", cm2.exception.args[0])


def get_ca_certificate() -> str:
    ca_cert_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "certs/ca/ca.crt"
    )
    with open(ca_cert_path) as f:
        return f.read()


def get_server_certificate(grpc_target: str) -> str:
    return ssl.get_server_certificate(
        addr=cast(tuple[str, int], grpc_target.split(":")),
    )


class KurrentDBClientTestCase(TimedTestCase):
    client: KurrentDBClient

    KDB_TARGET = "localhost:2114"
    KDB_TLS = True
    KDB_CLUSTER_SIZE = 1

    def construct_esdb_client(self, qs: str = "") -> None:
        if self.KDB_CLUSTER_SIZE > 1:
            qs = f"MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1&{qs}"
        if self.KDB_TLS:
            uri = f"kdb://admin:changeit@{self.KDB_TARGET}?{qs}"
            root_certificates = self.get_root_certificates()
        else:
            uri = f"kdb://{self.KDB_TARGET}?Tls=false&{qs}"
            root_certificates = None
        self.client = KurrentDBClient(uri, root_certificates=root_certificates)

    def get_root_certificates(self) -> str:
        if self.KDB_CLUSTER_SIZE == 1:
            return get_server_certificate(self.KDB_TARGET.split(",")[0])
        if self.KDB_CLUSTER_SIZE == 3:
            return get_ca_certificate()
        msg = f"Test doesn't work with cluster size {self.KDB_CLUSTER_SIZE}"
        raise ValueError(msg)

    def tearDown(self) -> None:
        try:
            if hasattr(self, "client") and not self.client.is_closed:
                for subscription in self.client.list_subscriptions():
                    self.client.delete_subscription(
                        group_name=subscription.group_name,
                        stream_name=(
                            None
                            if subscription.event_source == "$all"
                            else subscription.event_source
                        ),
                    )
                self.client.close()
        finally:
            super().tearDown()


class TestKurrentDBClient(KurrentDBClientTestCase):
    def test_context_manager(self) -> None:
        self.construct_esdb_client()
        self.assertFalse(self.client.is_closed)
        with self.client:
            self.assertFalse(self.client.is_closed)
        self.assertTrue(self.client.is_closed)

    def test_close(self) -> None:
        self.construct_esdb_client()
        self.assertFalse(self.client.is_closed)
        self.client.close()
        self.assertTrue(self.client.is_closed)
        self.client.close()
        self.assertTrue(self.client.is_closed)

        self.construct_esdb_client()
        self.assertFalse(self.client.is_closed)
        self.client.close()
        self.assertTrue(self.client.is_closed)
        self.client.close()
        self.assertTrue(self.client.is_closed)

    def test_stream_read_raises_not_found(self) -> None:
        # Note, we never get a NotFound from subscribe_to_stream(), which is
        # logical because the stream might be written after the subscription. So here
        # we just test get_stream().

        self.construct_esdb_client()
        stream_name = str(uuid4())

        read_response = self.client.read_stream(stream_name)
        with self.assertRaises(NotFoundError):
            tuple(read_response)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name, backwards=True)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name, stream_position=1)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name, stream_position=1, backwards=True)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name, limit=10)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name, backwards=True, limit=10)

        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name, stream_position=1, limit=10)

        with self.assertRaises(NotFoundError):
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

    def test_append_to_stream_wrong_credentials(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            metadata=random_data(),
            content_type="application/octet-stream",
        )
        with self.assertRaises(UnauthenticatedError):
            self.client.append_to_stream(
                stream_name=stream_name1,
                current_version=StreamState.ANY,
                events=[event1],
                credentials=self.client.construct_call_credentials("foo", "assword"),
            )

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
        cm: _AssertRaisesContext[Any]
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Check stream position is None.
        self.assertEqual(
            self.client.get_current_version(stream_name), StreamState.NO_STREAM
        )

        # Todo: Reintroduce this when/if testing for streaming individual events.
        # # Check get error when attempting to append empty list to position 1.
        # with self.assertRaises(WrongCurrentVersion) as cm:
        #     self.client.append_events(stream_name, current_version=1, events=[])
        # self.assertEqual(
        #     cm.exception.args[0],
        #     f"Stream {stream_name!r} does not exist",
        # )

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
        with self.assertRaises(WrongCurrentVersionError) as cm:
            self.client.append_event(stream_name, current_version=1, event=event1)
        self.assertEqual(cm.exception.args[0], f"Stream {stream_name!r} does not exist")

        # Append new event with correct expected position of StreamState.NO_STREAM.
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
        self.assertEqual(events[0].commit_position, commit_position1)

        # Check we can't append another new event at initial position.

        with self.assertRaises(WrongCurrentVersionError) as cm:
            self.client.append_event(
                stream_name, current_version=StreamState.NO_STREAM, event=event2
            )
        self.assertEqual(
            "Stream position of last event is 0 not StreamState.NO_STREAM",
            cm.exception.args[0],
        )

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
        with self.assertRaises(WrongCurrentVersionError) as cm:
            self.client.append_event(stream_name, current_version=0, event=event3)
        self.assertEqual(
            "Stream position of last event is 1 not 0", cm.exception.args[0]
        )

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
        with self.assertRaises(WrongCurrentVersionError):
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

        # Subsequent write of events 2, 3, and 4.
        self.client.append_events(
            stream_name,
            current_version=2,
            events=[event2, event3, event4],
        )

        # Stream now has several events with the same ID...
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

    def test_resolve_links_when_reading_from_dollar_et_projection(self) -> None:
        if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
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
        with self.assertRaises(WrongCurrentVersionError):
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
    #         stream_name,
    #         current_version=StreamState.NO_STREAM,
    #         events=[event1, event2]
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
    #     with self.assertRaises(WrongCurrentVersion):
    #         self.client.append_events_multiplexed(
    #             stream_name,
    #             current_version=StreamState.NO_STREAM,
    #             events=[event3, event4],
    #         )
    #
    #     # Fail to append (wrong expected position).
    #     with self.assertRaises(WrongCurrentVersion):
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
        with self.assertRaises(WrongCurrentVersionError):
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

        assert events[0].commit_position > commit_position0
        assert events[0].commit_position < commit_position2
        assert events[1].commit_position == commit_position2

        # Fail to append (stream already exists).
        event3 = NewEvent(type="OrderUpdated", data=random_data())
        event4 = NewEvent(type="OrderUpdated", data=random_data())
        with self.assertRaises(WrongCurrentVersionError):
            self.client.append_events(
                stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event3, event4],
            )

        # Fail to append (wrong expected position).
        with self.assertRaises(WrongCurrentVersionError):
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
        with self.assertRaises(WrongCurrentVersionError):
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
        with self.assertRaises(GrpcDeadlineExceededError):
            self.client.append_events(
                stream_name=stream_name1,
                current_version=StreamState.NO_STREAM,
                events=new_events,
                timeout=0,
            )

        with self.assertRaises(NotFoundError):
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

    def assert_filtered_events(
        self,
        commit_position: int,
        expected: set[str],
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        *,
        filter_by_stream_name: bool = False,
    ) -> None:
        events: list[RecordedEvent] = list(
            self.client.read_all(
                commit_position=commit_position,
                filter_exclude=filter_exclude,
                filter_include=filter_include,
                filter_by_stream_name=filter_by_stream_name,
            )
        )
        if filter_by_stream_name is False:
            actual = {e.type for e in events}
        else:
            actual = {e.stream_name for e in events}
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include="OrderCreated",
            expected={"OrderCreated"},
        )

        # Read only OrderCreated and OrderDeleted.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include=["OrderCreated", "OrderDeleted"],
            expected={"OrderCreated", "OrderDeleted"},
        )

        # Read only Order.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include="Order",
            expected=set(),
        )

        # Read only Updated.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include="Updated",
            expected=set(),
        )

        # Read only Order.*.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include="Order.*",
            expected={"OrderCreated", "OrderUpdated", "OrderDeleted"},
        )

        # Read only Invoice.*.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include="Invoice.*",
            expected={"InvoiceCreated", "InvoiceUpdated", "InvoiceDeleted"},
        )

        # Read only .*Created.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include=".*Created",
            expected={"OrderCreated", "InvoiceCreated"},
        )

        # Read only .*Updated.
        self.assert_filtered_events(
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, "OrderCreated"),
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, "OrderCreated", "OrderDeleted"),
            expected={
                "OrderUpdated",
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude Order. Should exclude nothing.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, "Order"),
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, "Created"),
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, r"Order.*"),
            expected={
                "InvoiceCreated",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude *.Created. Should exclude event1 and event4.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, r".*Created"),
            expected={
                "OrderUpdated",
                "OrderDeleted",
                "InvoiceUpdated",
                "InvoiceDeleted",
                "SomethingElse",
            },
        )

        # Exclude *.thing.*. Should exclude event7.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, r".*thing.*"),
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_exclude=(*DEFAULT_EXCLUDE_FILTER, r".OrderCreated.+"),
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=[stream_name1, stream_name2],
            expected={stream_name1, stream_name2},
        )

        # Read only stream2 and stream3.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=[stream_name2, stream_name3],
            expected={stream_name2, stream_name3},
        )

        # Read only prefix1.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_include=prefix1 + ".*",
            expected={stream_name1, stream_name2},
        )

        # Read only prefix2.
        self.assert_filtered_events(
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[KDB_SYSTEM_EVENTS_REGEX, stream_name1],
            expected={stream_name2, stream_name3},
        )

        # Read everything except stream2 and stream3.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[KDB_SYSTEM_EVENTS_REGEX, stream_name2, stream_name3],
            expected={stream_name1},
        )

        # Read everything except prefix1.*.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[KDB_SYSTEM_EVENTS_REGEX, prefix1 + ".*"],
            expected={stream_name3},
        )

        # Read everything except prefix2.*.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[KDB_SYSTEM_EVENTS_REGEX, prefix2 + ".*"],
            expected={stream_name1, stream_name2},
        )

        # Read everything except prefix2.
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_by_stream_name=True,
            filter_exclude=[KDB_SYSTEM_EVENTS_REGEX, prefix2],
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
        self.assert_filtered_events(
            commit_position=commit_position,
            filter_include=["OrderCreated"],
            filter_exclude=["OrderCreated"],
            expected={"OrderCreated"},
        )

    def test_read_all_filter_nothing(self) -> None:
        if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
            self.skipTest("This test doesn't work with this configuration")

        self.construct_esdb_client()

        # Read all events.
        read_response = self.client.read_all(filter_exclude=[])

        for event in read_response:
            if event.stream_name.startswith("$"):
                break

    # def test_read_all_filter_by_system_stream(self) -> None:
    #     # This was a response to Tony's query about filtering for secondary indexes.
    #     if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
    #         self.skipTest("This test doesn't work with this configuration")
    #
    #     self.construct_esdb_client()
    #
    #     # Read all events.
    #     read_response = self.client.read_all(
    #         filter_by_stream_name=True, filter_include=r"\$\$\$scavenges"
    #     )
    #     # read_response = self.client.get_stream("$$$scavenges")
    #
    #     for event in read_response:
    #         print(event)
    #         if event.stream_name.startswith("$"):
    #             break
    #     else:
    #         self.fail("Didn't get an event")

    def test_read_all_resolve_links(self) -> None:
        if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
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
        # See BaseReadResponse._convert_read_resp() where a
        # streams_pb2.ReadResp.ReadEvent a gives a
        # "link streams_pb2.ReadResp.ReadEvent.RecordedEvent" but not an
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
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(WrongCurrentVersionError):
            self.client.delete_stream(stream_name, current_version=0)

        # Delete the stream, specifying correct expected position.
        self.client.delete_stream(stream_name, current_version=1)

        # Can't call delete again with incorrect expected position.
        with self.assertRaises(WrongCurrentVersionError):
            self.client.delete_stream(stream_name, current_version=0)

        # Can call delete again, with correct expected position.
        self.client.delete_stream(stream_name, current_version=1)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Expect stream position is None.
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can append to a deleted stream.
        self.client.append_events(
            stream_name, current_version=StreamState.NO_STREAM, events=[event3]
        )
        # with self.assertRaises(WrongCurrentVersion):
        #     self.client.append_events(
        #         stream_name,
        #         current_version=StreamState.NO_STREAM,
        #         events=[event3]
        #     )
        # self.client.append_events(stream_name, current_version=1, events=[event3])

        sleep(0.1)  # sometimes we need to wait a little bit for KurrentDB
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
        with self.assertRaises(WrongCurrentVersionError):
            self.client.delete_stream(stream_name, current_version=0)

        # Can still read the events.
        self.assertEqual(3, self.client.get_current_version(stream_name))
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 2)

        # Can delete the stream again, using correct expected position.
        self.client.delete_stream(stream_name, current_version=3)

        # Stream is now "not found".
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can't call delete again with incorrect expected position.
        with self.assertRaises(WrongCurrentVersionError):
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
        with self.assertRaises(GrpcDeadlineExceededError):
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

    def test_get_commit_position(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position1 = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )

        commit_position2 = self.client.get_commit_position()
        self.assertEqual(commit_position1, commit_position2)

        commit_position3 = self.client.get_commit_position(filter_exclude=[".*"])
        self.assertEqual(0, commit_position3)

    def test_stream_delete_with_any_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Can't delete stream that doesn't exist, while expecting "any" version.
        # Todo: I don't fully understand why this should cause an error.
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Expect stream position is None.
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can append to a deleted stream.
        with self.assertRaises(WrongCurrentVersionError):
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
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Can't delete stream, expecting stream exists, because stream never existed.
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(StreamIsDeletedError):
            self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Expect stream position is None.
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can't append to a deleted stream with incorrect expected position.
        with self.assertRaises(WrongCurrentVersionError):
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
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)
        self.assertEqual(
            StreamState.NO_STREAM, self.client.get_current_version(stream_name)
        )

        # Can't call delete again, expecting stream exists, because it was deleted.
        with self.assertRaises(StreamIsDeletedError):
            self.client.delete_stream(stream_name, current_version=StreamState.EXISTS)

    def test_tombstone_stream_with_current_version(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(WrongCurrentVersionError):
            self.client.tombstone_stream(stream_name, current_version=0)

        # Tombstone the stream, specifying expected position.
        self.client.tombstone_stream(stream_name, current_version=1)

        # Can't tombstone again with correct expected position, stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
            self.client.tombstone_stream(stream_name, current_version=1)

        # Can't tombstone again with incorrect expected position, stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
            self.client.tombstone_stream(stream_name, current_version=2)

        # Can't read from stream, because stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
            self.client.get_stream(stream_name)

        # Can't get stream position, because stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
            self.client.get_current_version(stream_name)

        # Can't append to tombstoned stream, because stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
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
        with self.assertRaises(StreamIsDeletedError):
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
        with self.assertRaises(StreamIsDeletedError):
            self.client.tombstone_stream(stream_name2, current_version=StreamState.ANY)

        with self.assertRaises(StreamIsDeletedError):
            self.client.get_stream(stream_name2)

        with self.assertRaises(StreamIsDeletedError):
            self.client.get_current_version(stream_name2)

    def test_tombstone_stream_expecting_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Can't tombstone stream that doesn't exist, while expecting "stream exists".
        with self.assertRaises(NotFoundError):
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
        with self.assertRaises(StreamIsDeletedError):
            self.client.tombstone_stream(
                stream_name, current_version=StreamState.NO_STREAM
            )

        with self.assertRaises(StreamIsDeletedError):
            self.client.get_stream(stream_name)

        with self.assertRaises(StreamIsDeletedError):
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

    @skipIf(
        SERVER_VERSION <= (22, 10),
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    def test_subscribe_to_all_include_caught_up(self) -> None:
        self.construct_esdb_client()

        commit_position = self.client.get_commit_position()

        # Append new events.
        before_recording = datetime.datetime.now(tz=datetime.timezone.utc)
        event1 = NewEvent(type="OrderCreated", data=random_data())
        stream_name1 = str(uuid4())
        self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1],
        )

        # Subscribe excluding all events, with small window.
        subscription = self.client.subscribe_to_all(
            commit_position=commit_position,
            filter_exclude=".*",
            include_caught_up=True,
            timeout=10,
        )

        # Expect to get caught up message.
        for event in subscription:
            if isinstance(event, CaughtUp):
                if SERVER_VERSION == (23, 10):
                    pass
                else:
                    self.assertEqual(0, event.stream_position)
                    self.assertEqual(commit_position, event.commit_position)
                    self.assertEqual(commit_position, event.prepare_position)
                    assert event.recorded_at is not None
                    self.assertGreaterEqual(event.recorded_at, before_recording)
                    after_subscribing = datetime.datetime.now(tz=datetime.timezone.utc)
                    self.assertLessEqual(event.recorded_at, after_subscribing)
                break

    @skipIf(SERVER_VERSION >= (22, 10), "'Extra checkpoint' bug was fixed")
    def test_demonstrate_extra_checkpoint_bug(self) -> None:
        self.construct_esdb_client()

        initial_commit_position = self.client.get_commit_position()

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

        def count_events_from_commit_position(commit_position: int) -> int:
            read_response = self.client.read_all(
                commit_position=commit_position,
                filter_exclude=[],
            )
            events = tuple(read_response)
            return len(events)

        self.assertEqual(
            1, count_events_from_commit_position(first_append_commit_position)
        )

        # Subscribe to all events, large window to discourage checkpoint generation.
        subscription1 = self.client.subscribe_to_all(
            commit_position=initial_commit_position,
            include_checkpoints=True,
            window_size=10000,
            checkpoint_interval_multiplier=500,
            timeout=1,
        )

        # Iterate over subscription, remembering last checkpoint commit position.
        last_checkpoint_commit_position: int | None = None
        last_event: RecordedEvent | None = None
        try:
            for _event in subscription1:
                if isinstance(_event, Checkpoint):
                    last_checkpoint_commit_position = _event.commit_position
                elif isinstance(_event, RecordedEvent):
                    last_event = _event
        except DeadlineExceededError:
            pass

        # Check we got an event.
        assert last_event is not None

        # Check the last event is event3.
        self.assertEqual(last_event.id, event3.id, last_event)

        # Check we got a checkpoint.
        assert last_checkpoint_commit_position is not None

        # Show checkpoint commit position is greater than current commit position.
        self.assertGreater(
            last_checkpoint_commit_position,
            self.client.get_commit_position(filter_exclude=[]),
        )

        # Append another event.
        event4 = NewEvent(type="OrderCreated", data=random_data())
        stream_name2 = str(uuid4())
        second_append_commit_position = self.client.append_events(
            stream_name2,
            current_version=StreamState.NO_STREAM,
            events=[event4],
        )

        # Show the checkpoint commit position is allocated to the next appended event.
        self.assertEqual(second_append_commit_position, last_checkpoint_commit_position)

        # Append another event.
        event5 = NewEvent(type="OrderCreated", data=random_data())
        stream_name3 = str(uuid4())
        self.client.append_events(
            stream_name3,
            current_version=StreamState.NO_STREAM,
            events=[event5],
        )

        # Subscribe from the last checkpoint commit position.
        subscription2 = self.client.subscribe_to_all(
            commit_position=last_checkpoint_commit_position
        )
        first_event_from_subscription2 = next(subscription2)

        # Show the first event from subscription2 is event5, and not event4.
        self.assertEqual(first_event_from_subscription2.id, event5.id)
        self.assertNotEqual(first_event_from_subscription2.id, event4.id)
        # Et voila! Resuming a subscription from the checkpoint commit position would
        # mean that an event is recorded and not received.

        # Show there are two events from the checkpoint commit position.
        self.assertEqual(
            count_events_from_commit_position(last_checkpoint_commit_position), 2
        )

    @skipIf(SERVER_VERSION <= (21, 10), "'Extra checkpoint' bug not fixed")
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

        def get_event_at_commit_position(
            commit_position: int,
        ) -> RecordedEvent | None:
            read_response = self.client.read_all(
                commit_position=commit_position,
                # backwards=True,
                filter_exclude=[],
                limit=1,
            )
            events = tuple(read_response)
            if len(events) == 1:
                event = events[0]
                assert event.commit_position == commit_position, event
                return event
            return None

        event = get_event_at_commit_position(first_append_commit_position)
        self.assertIsNotNone(event)
        assert event is not None  # for mypy
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

        # We shouldn't get an extra checkpoint at the end (bug with <v23.10),
        # that has a commit position greater than the current commit position (v24.2).
        checkpoint_commit_position: int | None = None
        try:
            for event in subscription1:
                if isinstance(event, Checkpoint):
                    checkpoint_commit_position = event.commit_position
                    # break
        except GrpcDeadlineExceededError:
            pass

        fail_msg = ""

        if (
            checkpoint_commit_position is not None
            and checkpoint_commit_position > current_commit_position
        ):
            fail_msg = (
                f"Server has 'extra checkpoint' bug ("
                f"checkpoint commit position: {checkpoint_commit_position}, "
                f"current commit position: {current_commit_position}."
            )

        if fail_msg:
            assert checkpoint_commit_position is not None  # for mypy
            event_at_checkpoint_commit_position = get_event_at_commit_position(
                checkpoint_commit_position
            )
            if event_at_checkpoint_commit_position is None:
                fail_msg += " No event found at checkpoint commit position."
            else:
                fail_msg += (
                    f" Event at checkpoint commit position:"
                    f" {event_at_checkpoint_commit_position}."
                )

            if event_at_checkpoint_commit_position is None:
                event4 = NewEvent(type="OrderUndeleted", data=random_data())
                second_append_commit_position = self.client.append_events(
                    stream_name1,
                    current_version=2,
                    events=[event4],
                )
                if second_append_commit_position == checkpoint_commit_position:
                    fail_msg += (
                        " Checkpoint commit position was used for "
                        "subsequently recorded event."
                    )
                elif second_append_commit_position > checkpoint_commit_position:
                    fail_msg += (
                        " Checkpoint commit position was less than "
                        "commit position of subsequently recorded event."
                    )
                else:
                    fail_msg += (
                        " Checkpoint commit position was greater than "
                        "commit position of subsequently recorded event."
                    )

        if fail_msg:
            self.fail(fail_msg)

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
            with self.assertRaises(GrpcDeadlineExceededError):
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
        with self.assertRaises(ConsumerTooSlowError):
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
            except ConsumerTooSlowError:
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

    @skipIf(
        SERVER_VERSION <= (22, 10),
        "Server doesn't support 'caught up' or 'fell behind' messages",
    )
    def test_subscribe_to_stream_include_caught_up(self) -> None:
        self.construct_esdb_client()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append new events.
        before_recording = datetime.datetime.now(tz=datetime.timezone.utc)
        stream_name1 = str(uuid4())
        commit_position = self.client.append_events(
            stream_name1,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )

        # Subscribe to stream events, from the start.
        subscription = self.client.subscribe_to_stream(
            stream_name=stream_name1,
            include_caught_up=True,
            timeout=10,
        )
        for event in subscription:
            if isinstance(event, CaughtUp):
                if SERVER_VERSION == (23, 10):
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

        # Sleep before calling replay_parked_events() so KurrentDB catches up.
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
        with self.assertRaises(ExceptionIteratingRequestsError) as cm:
            for _ in subscription:
                subscription.ack(NotUUID())  # type: ignore
        self.assertIsInstance(cm.exception.__cause__, ValueError)

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

        # Sleep before calling replay_parked_events() so KurrentDB catches up.
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

    def test_persistent_subscription_ack_after_stop(self) -> None:
        self.construct_esdb_client()

        group_name = str(uuid4())
        self.client.create_subscription_to_all(group_name)

        # Can't ack after subscription has been stopped (not context manager).
        s = self.client.read_subscription_to_all(group_name)
        s.stop()
        with self.assertRaises(ProgrammingError):
            s.ack(uuid4())
        with self.assertRaises(ProgrammingError):
            s.nack(uuid4(), "retry")

        # Can ack after subscription has been stopped (is context manager).
        s = self.client.read_subscription_to_all(group_name)
        with s:
            s.stop()
            s.ack(uuid4())
            s.nack(uuid4(), "retry")

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

    # def test_subscription_to_all_with_message_timeout_consumer_crashes_and_resumes(
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
    #     # If we don't stop(), then subscription2 is severely delayed.
    #     subscription1.stop()
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
        if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
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
        if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
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

    def test_subscription_to_all_raises_maximum_subscriptions_reached(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name1 = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription_to_all(
            group_name=group_name1, max_subscriber_count=2, from_end=True
        )

        # Multiple consumers.
        subscription1 = self.client.read_subscription_to_all(group_name=group_name1)
        subscription2 = self.client.read_subscription_to_all(group_name=group_name1)
        with self.assertRaises(MaximumSubscriptionsReachedError):
            self.client.read_subscription_to_all(group_name=group_name1)
        subscription1.stop()
        subscription2.stop()

    def test_subscription_get_info(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(NotFoundError):
            self.client.get_subscription_info(group_name)
        # Create persistent subscription.
        self.client.create_subscription_to_all(group_name=group_name)

        info = self.client.get_subscription_info(group_name)
        self.assertEqual(info.group_name, group_name)
        self.assertEqual(info.event_source, "$all")
        self.assertEqual(len(info.connections), 0)

        _read_response = self.client.read_subscription_to_all(group_name=group_name)
        info = self.client.get_subscription_info(group_name)
        self.assertEqual(len(info.connections), 1)
        connection_info = info.connections[0]
        self.assertIsInstance(connection_info, ConnectionInfo)
        self.assertEqual(connection_info.from_, "")

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
        with self.assertRaises(AlreadyExistsError):
            self.client.create_subscription_to_all(group_name)

    def test_subscription_to_all_update(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        # Can't update subscription that doesn't exist.
        with self.assertRaises(NotFoundError):
            # raises in get_info()
            self.client.update_subscription_to_all(group_name=group_name)
        with self.assertRaises(NotFoundError):
            # raises in update()
            self.client._connection.persistent_subscriptions.update(
                group_name=group_name,
                metadata=self.client._call_metadata,
                credentials=self.client._call_credentials,
            )

        # Create persistent subscription with defaults.
        self.client.create_subscription_to_all(
            group_name=group_name,
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, resolve_links=True
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, consumer_strategy="RoundRobin"
        )

        info = self.client.get_subscription_info(group_name=group_name)
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

        self.client.update_subscription_to_all(
            group_name=group_name, consumer_strategy="Pinned"
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, message_timeout=15.0
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(group_name=group_name, max_retry_count=5)

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, min_checkpoint_count=7
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, max_checkpoint_count=12
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, checkpoint_after=1.0
        )
        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, max_subscriber_count=10
        )
        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, live_buffer_size=300
        )
        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, read_batch_size=250
        )
        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, history_buffer_size=400
        )
        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name, extra_statistics=True
        )
        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(group_name=group_name, from_end=True)

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(group_name=group_name)

        info = self.client.get_subscription_info(group_name=group_name)
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
        commit_position = self.client.get_commit_position()
        self.client.update_subscription_to_all(
            group_name=group_name,
            commit_position=commit_position,
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name,
        )

        info = self.client.get_subscription_info(group_name=group_name)
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
        self.client.update_subscription_to_all(
            group_name=group_name,
            from_end=False,
        )

        info = self.client.get_subscription_info(group_name=group_name)
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

    @skipIf(
        SERVER_VERSION <= (21, 10),
        "v21.10 server becomes unresponsive with this test",
    )
    def test_subscription_to_all_wrong_history_buffer_size_raises_internal_error(
        self,
    ) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"

        with self.assertRaises(InternalError):
            self.client.create_subscription_to_all(
                group_name=group_name,
                history_buffer_size=100,
            )

        self.client.create_subscription_to_all(
            group_name=group_name,
        )
        with self.assertRaises(InternalError):
            self.client.update_subscription_to_all(
                group_name=group_name,
                history_buffer_size=100,
            )

    def test_subscription_to_all_invalid_consumer_strategy(
        self,
    ) -> None:
        self.construct_esdb_client()

        group_name = f"my-group-{uuid4().hex}"

        with self.assertRaises(InternalError):
            self.client.create_subscription_to_all(  # type: ignore[call-overload]
                group_name=group_name, consumer_strategy="InvalidStrategy"
            )

        self.client.create_subscription_to_all(
            group_name=group_name,
        )

        with self.assertRaises(KeyError):
            self.client.update_subscription_to_all(  # type: ignore[call-overload]
                group_name=group_name, consumer_strategy="InvalidStrategy"
            )

    def test_subscription_delete(self) -> None:
        self.construct_esdb_client()

        group_name = f"my-subscription-{uuid4().hex}"

        # Can't delete a subscription that doesn't exist.
        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(NotFoundError):
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

        group_name = f"my-subscription-{uuid4().hex}"
        stream_name = f"my-stream-{uuid4().hex}"

        # Can't update subscription that doesn't exist.
        with self.assertRaises(NotFoundError):
            self.client.update_subscription_to_stream(
                group_name=group_name,
                stream_name=stream_name,
            )

        # Append an event.
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}")
        self.client.append_events(
            stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )

        # Create persistent subscription with defaults.
        self.client.create_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, resolve_links=True
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            consumer_strategy="RoundRobin",
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, message_timeout=15.0
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_retry_count=5
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, min_checkpoint_count=7
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_checkpoint_count=12
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, checkpoint_after=1.0
        )
        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, max_subscriber_count=10
        )
        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, live_buffer_size=300
        )
        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, read_batch_size=250
        )
        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, history_buffer_size=400
        )
        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, extra_statistics=True
        )
        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name, from_end=True
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        info = self.client.get_subscription_info(
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
        stream_position = self.client.get_current_version(stream_name)
        assert isinstance(stream_position, int)
        self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            stream_position=stream_position,
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
        )

        info = self.client.get_subscription_info(
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
        self.client.update_subscription_to_stream(
            group_name=group_name,
            stream_name=stream_name,
            from_end=False,
        )

        info = self.client.get_subscription_info(
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

    def test_subscription_to_stream_raises_maximum_subscriptions_reached(self) -> None:
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
        with self.assertRaises(MaximumSubscriptionsReachedError):
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
        with self.assertRaises(AlreadyExistsError):
            self.client.create_subscription_to_stream(group_name, stream_name)

    def test_subscription_to_stream_delete(self) -> None:
        self.construct_esdb_client()

        stream_name = str(uuid4())
        group_name = f"my-subscription-{uuid4().hex}"

        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(NotFoundError):
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
        if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
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
    #     if self.KDB_CLUSTER_SIZE > 1 or self.KDB_TLS is not True:
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
        with self.assertRaises(NotFoundError):
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

        with self.assertRaises(WrongCurrentVersionError):
            self.client.set_stream_metadata(
                stream_name=stream_name,
                metadata=metadata,
                current_version=10,
            )

        self.client.tombstone_stream(stream_name, current_version=StreamState.ANY)

        # Can't get metadata after tombstoning stream, because stream is deleted.
        with self.assertRaises(StreamIsDeletedError):
            self.client.get_stream_metadata(stream_name)

        # For some reason, we can set stream metadata, even though the stream
        # has been tombstoned, and even though we can't get stream metadata.
        # Todo: Ask DB team why this is?
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

        with self.assertRaises(StreamIsDeletedError):
            self.client.get_stream_metadata(stream_name)

    def test_gossip_read(self) -> None:
        self.construct_esdb_client()
        if self.KDB_CLUSTER_SIZE == 1:
            cluster_info = self.client.read_gossip()
            self.assertEqual(len(cluster_info), 1)
            expected_address = self.KDB_TARGET.split(":")[0]
            expected_port = int(self.KDB_TARGET.split(":")[1])
            self.assertEqual(cluster_info[0].state, NODE_STATE_LEADER)
            self.assertEqual(cluster_info[0].address, expected_address)
            self.assertEqual(cluster_info[0].port, expected_port)
        elif self.KDB_CLUSTER_SIZE == 3:
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
            self.fail(f"Test doesn't work with cluster size {self.KDB_CLUSTER_SIZE}")

    def test_create_projection(self) -> None:
        self.construct_esdb_client()
        # Create "continuous" projection.
        projection_name = str(uuid4())
        self.client.create_projection(query="", name=projection_name)

        # Create "continuous" projection (emit enabled).
        projection_name = str(uuid4())
        self.client.create_projection(
            query="",
            name=projection_name,
            emit_enabled=True,
        )

        # Create "continuous" projection (track emitted streams).
        projection_name = str(uuid4())
        self.client.create_projection(
            query="",
            name=projection_name,
            emit_enabled=True,
            track_emitted_streams=True,
        )

        # Raises error if projection already exists.
        with self.assertRaises(AlreadyExistsError):
            self.client.create_projection(
                query="",
                name=projection_name,
                emit_enabled=True,
                track_emitted_streams=True,
            )

        # Raises error if track_emitted=True but emit_enabled=False...
        with self.assertRaises(ExceptionThrownByHandlerError):
            self.client.create_projection(
                query="",
                name=projection_name,
                emit_enabled=False,
                track_emitted_streams=True,
            )

    def test_update_projection(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.update_projection(name=projection_name, query="")

        # Create named projection.
        self.client.create_projection(query="", name=projection_name)

        # Update projection.
        self.client.update_projection(name=projection_name, query="")
        self.client.update_projection(name=projection_name, query="", emit_enabled=True)
        self.client.update_projection(
            name=projection_name, query="", emit_enabled=False
        )

    def test_delete_projection(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.delete_projection(projection_name)

        # Create named projection.
        self.client.create_projection(query="", name=projection_name)

        # Delete projection.
        self.client.delete_projection(
            name=projection_name,
            delete_emitted_streams=True,
            delete_state_stream=True,
            delete_checkpoint_stream=True,
        )

        sleep(1)  # give server time to actually delete the projection....

        if SERVER_VERSION in [(21, 10), (22, 10)]:
            # Can delete a projection that has been deleted ("idempotent").
            self.client.delete_projection(
                name=projection_name,
            )
        else:
            # Can't delete a projection that has been deleted.
            with self.assertRaises(NotFoundError):
                self.client.delete_projection(
                    name=projection_name,
                )

    def test_get_projection_statistics(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.get_projection_statistics(name=projection_name)

        # Create named projection.
        self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual(projection_name, statistics.name)

    # @skip("Listing projection statistics is flaky (error thrown by handler)")
    # # Todo: Figure out why this sometimes works and sometimes doesn't.
    def test_list_all_projection_statistics(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Create named projection.
        self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )
        sleep(0.5)

        statistics = self.client.list_all_projection_statistics()
        self.assertIsInstance(statistics, list)
        self.assertGreater(len(statistics), 0)
        self.assertIsInstance(statistics[0], ProjectionStatistics)

    # @skip("Listing projection statistics is flaky (error thrown by handler)")
    # # Todo: Figure out why this sometimes works and sometimes doesn't.
    def test_list_continuous_projection_statistics(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Create named projection.
        self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        sleep(0.5)
        statistics = self.client.list_continuous_projection_statistics()
        self.assertIsInstance(statistics, list)
        self.assertGreater(len(statistics), 0)
        self.assertIsInstance(statistics[0], ProjectionStatistics)

    def test_disable_projection(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.disable_projection(name=projection_name)

        # Create named projection.
        self.client.create_projection(query="", name=projection_name)

        # Disable projection.
        self.client.disable_projection(name=projection_name)

    def test_abort_projection(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.disable_projection(name=projection_name)

        # Create named projection.
        self.client.create_projection(query="", name=projection_name)

        # Abort projection.
        self.client.abort_projection(name=projection_name)

    def test_enable_projection(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.enable_projection(name=projection_name)

        # Create named projection.
        self.client.create_projection(query="", name=projection_name)

        # Disable projection.
        self.client.enable_projection(name=projection_name)

    def test_reset_projection(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.reset_projection(name=projection_name)

        # Create named projection.
        self.client.create_projection(query="", name=projection_name)

        # Reset projection.
        self.client.reset_projection(name=projection_name)

    def test_get_projection_state(self) -> None:
        self.construct_esdb_client()
        projection_name = str(uuid4())

        # Raises NotFound unless projection exists.
        with self.assertRaises(NotFoundError):
            self.client.get_projection_state(name=projection_name)

        # Create named projection (query is an empty string).
        self.client.create_projection(query="", name=projection_name)

        # Try to get projection state.
        # Todo: Why does this just hang?
        with self.assertRaises(DeadlineExceededError):
            self.client.get_projection_state(name=projection_name, timeout=1)

        # Create named projection.
        projection_name = str(uuid4())
        self.client.create_projection(
            query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
            name=projection_name,
        )

        # Get projection state.
        state = self.client.get_projection_state(name=projection_name)
        self.assertEqual(state.value, {})

    def test_get_projection_state_partition(self) -> None:
        self.construct_esdb_client()
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
        self.client.append_events(
            stream_name=stream_name,
            events=events,
            current_version=StreamState.ANY,
        )

        # Create named projection (query is an empty string).
        self.client.create_projection(query=projection_query, name=projection_name)

        statistics = self.client.get_projection_statistics(name=projection_name)

        # Wait for four events to have been processed.
        for _ in range(100):
            if statistics.events_processed_after_restart < 3:
                sleep(0.1)
                statistics = self.client.get_projection_statistics(name=projection_name)
                continue
            break
        else:
            self.fail(
                "Timed out waiting for three events to be processed by projection"
            )

        state = self.client.get_projection_state(name=projection_name, partition="1")
        self.assertEqual(1, state.value["count"])
        state = self.client.get_projection_state(name=projection_name, partition="2")
        self.assertEqual(1, state.value["count"])
        state = self.client.get_projection_state(name=projection_name, partition="3")
        self.assertEqual(2, state.value["count"])

    # def test_get_projection_result(self) -> None:
    #     self.construct_esdb_client()
    #     projection_name = str(uuid4())
    #
    #     # Raises NotFound unless projection exists.
    #     with self.assertRaises(NotFound):
    #         self.client.get_projection_result(name=projection_name)
    #
    #     # Create named projection.
    #     self.client.create_projection(query="", name=projection_name)
    #
    #     # Try to get projection result.
    #     # Todo: Why does this just hang?
    #     with self.assertRaises(DeadlineExceeded):
    #         self.client.get_projection_result(name=projection_name, timeout=1)
    #
    #     # Create named projection.
    #     projection_name = str(uuid4())
    #     self.client.create_projection(
    #         query=PROJECTION_QUERY_TEMPLATE1 % ("app-" + projection_name),
    #         name=projection_name,
    #     )
    #
    #     # Get projection result.
    #     state = self.client.get_projection_result(name=projection_name)
    #     self.assertEqual(state.value, {})

    def test_restart_projections_subsystem(self) -> None:
        self.construct_esdb_client()
        self.client.restart_projections_subsystem()

    def test_projection_example(self) -> None:
        self.construct_esdb_client()

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
              count: 0,
              list: [null, "2.10", true]
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

        self.client.create_projection(
            query=projection_query,
            name=projection_name,
            emit_enabled=True,
            track_emitted_streams=True,
        )
        self.client.disable_projection(name=projection_name)

        # Set emit_enabled=False - still tracking emitted streams...
        self.client.update_projection(
            query=projection_query,
            name=projection_name,
            emit_enabled=False,
        )

        # Set emit_enabled=True again - still tracking emitted streams...
        self.client.update_projection(
            query=projection_query,
            name=projection_name,
            emit_enabled=True,
        )

        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual(projection_name, statistics.name)

        # Start running...
        self.client.enable_projection(name=projection_name)

        application_events = [
            NewEvent(type="SomethingHappened", data=b"{}"),
            NewEvent(type="SomethingElseHappened", data=b"{}"),
            NewEvent(type="SomethingHappened", data=b"{}"),
        ]
        self.client.append_events(
            stream_name=application_stream_name,
            events=application_events,
            current_version=StreamState.ANY,
        )

        # Wait for two events to have been processed.
        for _ in range(100):
            if statistics.events_processed_after_restart < 2:
                sleep(0.1)
                statistics = self.client.get_projection_statistics(name=projection_name)
                continue
            break
        else:
            self.fail("Timed out waiting for two events to be processed by projection")

        # Check projection state.
        state = self.client.get_projection_state(projection_name)
        self.assertEqual(2, state.value["count"])
        self.assertEqual([None, "2.10", True], state.value["list"])

        # Check projection result.
        # Todo: What's the actual difference between "state" and "result"?
        #  Ans: nothing, at the moment.
        # result = self.client.get_projection_result(projection_name)
        # self.assertEqual(2, result.value["count"])

        # Check project result stream.
        result_stream_name = f"$projections-{projection_name}-result"
        result_events = self.client.get_stream(result_stream_name)
        self.assertEqual(2, len(result_events))
        self.assertEqual("Result", result_events[0].type)
        self.assertEqual("Result", result_events[1].type)

        self.assertEqual(
            {"count": 1, "list": [None, "2.10", True]},
            json.loads(result_events[0].data),
        )
        self.assertEqual(
            {"count": 2, "list": [None, "2.10", True]},
            json.loads(result_events[1].data),
        )

        self.assertEqual(
            str(application_events[0].id),
            json.loads(result_events[0].metadata)["$causedBy"],
        )
        self.assertEqual(
            str(application_events[2].id),
            json.loads(result_events[1].metadata)["$causedBy"],
        )

        # Check emitted event stream.
        emitted_events = self.client.get_stream(emitted_stream_name)
        self.assertEqual(2, len(emitted_events))

        # Check projection statistics.
        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Running", statistics.status)

        # Reset whilst running is ineffective (state exists).
        self.client.reset_projection(name=projection_name)
        sleep(1)
        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Running", statistics.status)
        self.assertLess(0, statistics.events_processed_after_restart)

        state = self.client.get_projection_state(projection_name)
        self.assertIn("count", state.value)
        # result = self.client.get_projection_result(projection_name)
        # self.assertIn("count", result.value)

        # Can't delete whilst running.
        with self.assertRaises(OperationFailedError):
            self.client.delete_projection(
                projection_name,
                delete_emitted_streams=True,
                delete_state_stream=True,
                delete_checkpoint_stream=True,
            )

        # Disable projection (stop running).
        self.client.disable_projection(projection_name)
        sleep(1)
        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Stopped", statistics.status)

        # Check projection still has state.
        state = self.client.get_projection_state(projection_name)
        self.assertIn("count", state.value)
        # result = self.client.get_projection_result(projection_name)
        # self.assertIn("count", result.value)

        # Reset whilst stopped is effective (loses state)?
        self.client.reset_projection(name=projection_name)
        sleep(1)
        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Stopped", statistics.status)
        self.assertEqual(0, statistics.events_processed_after_restart)

        state = self.client.get_projection_state(projection_name)
        self.assertNotIn("count", state.value)
        # result = self.client.get_projection_result(projection_name)
        # self.assertNotIn("count", result.value)

        # Can enable after reset.
        self.client.enable_projection(name=projection_name)
        sleep(1)
        statistics = self.client.get_projection_statistics(name=projection_name)
        self.assertEqual("Running", statistics.status)
        state = self.client.get_projection_state(projection_name)
        self.assertIn("count", state.value)
        self.assertEqual(2, state.value["count"])

        # Can delete when stopped.
        self.client.disable_projection(name=projection_name)
        self.client.delete_projection(
            projection_name,
            delete_emitted_streams=True,
            delete_state_stream=True,
            delete_checkpoint_stream=True,
        )

        # Flaky: try/except because the projection might have been deleted already...
        try:
            statistics = self.client.get_projection_statistics(name=projection_name)
            self.assertEqual("Deleting/Stopped", statistics.status)
        except NotFoundError:
            pass

        sleep(1)

        # After deleting, projection methods raise NotFound.
        with self.assertRaises(NotFoundError):
            self.client.get_projection_statistics(name=projection_name)

        with self.assertRaises(NotFoundError):
            self.client.get_projection_state(projection_name)

        # with self.assertRaises(NotFound):
        #     self.client.get_projection_result(projection_name)

        with self.assertRaises(NotFoundError):
            self.client.enable_projection(projection_name)

        with self.assertRaises(NotFoundError):
            self.client.disable_projection(projection_name)

        # Result stream still exists.
        result_events = self.client.get_stream(result_stream_name)
        self.assertEqual(2, len(result_events))

        # Emitted stream does not exist.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(emitted_stream_name)

        # Todo: Are "checkpoint" and "state" streams somehow hidden?

        # Todo: Recreate with same name (plus what happens if streams not deleted)...
        # self.client.create_projection(name=projection_name, query=projection_query)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_one_stream(self) -> None:
        cm: _AssertRaisesContext[Any]
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(NotFoundError):
            self.client.get_stream(stream_name)

        # Check stream position is None.
        self.assertEqual(
            self.client.get_current_version(stream_name), StreamState.NO_STREAM
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
            self.client.multi_append_to_stream(
                NewEvents(stream_name, current_version=1, events=[event1])
            )
        self.assertEqual(
            f"Append failed due to a version conflict on stream {stream_name!r}. "
            f"Expected version: 1. Actual version: -1.",
            cm.exception.args[0],
        )

        # Check get error when attempting to append new event expecting stream exists.
        with self.assertRaises(WrongCurrentVersionError) as cm:
            self.client.multi_append_to_stream(
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
        with self.assertRaises(ProgrammingError) as cm:
            self.client.multi_append_to_stream(
                NewEvents(stream_name, current_version=-1, events=[event1])
            )
        self.assertEqual("Unsupported current_version value: -1", cm.exception.args[0])

        # Append new event with correct expected position of StreamState.NO_STREAM.
        commit_position0 = self.client.get_commit_position()
        commit_position1 = self.client.multi_append_to_stream(
            NewEvents(
                stream_name, current_version=StreamState.NO_STREAM, events=[event1]
            )
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
        self.assertEqual(events[0].content_type, event1.content_type)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[0].stream_name, stream_name)
        self.assertEqual(events[0].stream_position, 0)
        self.assertEqual(events[0].commit_position, commit_position1)

        # Check we can't append another new event at initial position.
        with self.assertRaises(WrongCurrentVersionError) as cm:
            self.client.multi_append_to_stream(
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
        commit_position2 = self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=0, events=[event2])
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
        with self.assertRaises(WrongCurrentVersionError) as cm:
            self.client.multi_append_to_stream(
                NewEvents(stream_name, current_version=0, events=[event3])
            )
        self.assertEqual(
            f"Append failed due to a version conflict on stream '{stream_name}'. "
            f"Expected version: 0. Actual version: 1.",
            cm.exception.args[0],
        )

        # Append another new event.
        commit_position3 = self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=1, events=[event3])
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
        commit_position1_1 = self.client.multi_append_to_stream(
            NewEvents(
                stream_name, current_version=StreamState.NO_STREAM, events=[event1]
            )
        )
        self.assertEqual(commit_position1, commit_position1_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event2.
        commit_position2_1 = self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=0, events=[event2])
        )
        self.assertEqual(commit_position2_1, commit_position2)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event3.
        commit_position3_1 = self.client.multi_append_to_stream(
            NewEvents(stream_name, current_version=1, events=[event3])
        )
        self.assertEqual(commit_position3, commit_position3_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event1, event2.
        commit_position2_1 = self.client.multi_append_to_stream(
            NewEvents(
                stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event1, event2],
            )
        )
        self.assertEqual(commit_position2, commit_position2_1)

        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Idempotent write of event2, event3.
        commit_position3_1 = self.client.multi_append_to_stream(
            NewEvents(
                stream_name,
                current_version=0,
                events=[event2, event3],
            )
        )
        self.assertEqual(commit_position3, commit_position3_1)

        # Stream should still have 3 events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Mixture of "idempotent" write of event2, event3, with new event4.
        with self.assertRaises(WrongCurrentVersionError):
            self.client.multi_append_to_stream(
                NewEvents(
                    stream_name,
                    current_version=0,
                    events=[event2, event3, event4],
                )
            )

        # Stream should still have 3 events.
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)

        # Append events with same ID at end of stream (specify current version).
        self.client.multi_append_to_stream(
            NewEvents(stream_name, [event2, event3, event4], 2)
        )

        # Stream now has 6 events....
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

        # Append events with same ID at end of stream (specify stream exists).
        self.client.multi_append_to_stream(
            NewEvents(stream_name, [event2, event1], StreamState.EXISTS)
        )

        # Stream still has 6 events....
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

        # Append events with same ID at end of stream (disable OCC).
        self.client.multi_append_to_stream(
            NewEvents(stream_name, [event2, event1], StreamState.ANY)
        )

        # Stream still has 6 events....
        events = self.client.get_stream(stream_name)
        self.assertEqual(len(events), 6)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)
        self.assertEqual(events[2].id, event3.id)
        self.assertEqual(events[3].id, event2.id)
        self.assertEqual(events[4].id, event3.id)
        self.assertEqual(events[5].id, event4.id)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_many_streams(self) -> None:
        self.construct_esdb_client()
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
        self.client.multi_append_to_stream(
            [
                NewEvents(stream_name1, [event1], StreamState.NO_STREAM),
                NewEvents(stream_name2, [event2], StreamState.NO_STREAM),
            ]
        )

        # Expect each stream has one event.
        events = self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        events = self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Idempotent append.
        self.client.multi_append_to_stream(
            [
                NewEvents(stream_name1, [event1], StreamState.NO_STREAM),
                NewEvents(stream_name2, [event2], StreamState.NO_STREAM),
            ]
        )

        # Expect each stream still has one event.
        events = self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        events = self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Append errors.
        with self.assertRaises(WrongCurrentVersionError):
            self.client.multi_append_to_stream(
                [
                    NewEvents(stream_name1, [event3], StreamState.NO_STREAM),
                    NewEvents(stream_name2, [event4], StreamState.NO_STREAM),
                ]
            )

        with self.assertRaises(WrongCurrentVersionError):
            self.client.multi_append_to_stream(
                [
                    NewEvents(stream_name1, [event3], 0),
                    NewEvents(stream_name2, [event4], StreamState.NO_STREAM),
                ]
            )

        with self.assertRaises(WrongCurrentVersionError):
            self.client.multi_append_to_stream(
                [
                    NewEvents(stream_name1, [event3], StreamState.NO_STREAM),
                    NewEvents(stream_name2, [event4], 0),
                ]
            )

        # Expect each stream still has one event.
        events = self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event1.id)

        events = self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event2.id)

        # Correct append to existing multi-streams.
        self.client.multi_append_to_stream(
            [
                NewEvents(stream_name1, [event3], 0),
                NewEvents(stream_name2, [event4], 0),
            ]
        )

        # Expect each stream now has two events.
        events = self.client.get_stream(stream_name1)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event3.id)

        events = self.client.get_stream(stream_name2)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event2.id)
        self.assertEqual(events[1].id, event4.id)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_wrong_credentials(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        with self.assertRaises(UnauthenticatedError):
            self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event1],
                    current_version=StreamState.NO_STREAM,
                ),
                credentials=self.client.construct_call_credentials("foo", "assword"),
            )

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_stream_already_exists_error(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        self.client.multi_append_to_stream(
            events=NewEvents(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            ),
        )
        # This doesn't fail because its treated as idempotent.
        self.client.multi_append_to_stream(
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
            self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event2],
                    current_version=StreamState.NO_STREAM,
                ),
            )
        self.assertEqual(cm.exception.stream_name, stream_name1)
        self.assertEqual(cm.exception.current_version, 0)
        self.assertEqual(cm.exception.expected_version, -1)

    @skip("This works but clogs the server and doesn't increase test coverage")
    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_record_max_size_exceeded_error(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        with self.assertRaises(RecordMaxSizeExceededError) as cm:
            size = 16
            while True:
                event1 = NewEvent(
                    type="OrderCreated",
                    data=random_data(size),
                    content_type="application/octet-stream",
                )
                self.client.multi_append_to_stream(
                    events=NewEvents(
                        stream_name=stream_name1,
                        events=[event1],
                        current_version=StreamState.ANY,
                    ),
                )
                size *= 2

        self.assertEqual(cm.exception.stream_name, stream_name1)
        self.assertEqual(cm.exception.event_id, event1.id)
        self.assertGreater(cm.exception.size, size)
        self.assertLess(cm.exception.max_size, cm.exception.size)

    @skip("This workds but clogs the server and doesn't increase test coverage")
    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_transaction_max_size_exceeded_error(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        data_size = 10 * 1024 * 1024
        num_events = 16

        with self.assertRaises(TransactionMaxSizeExceededError) as cm:

            def generate_events(num_events: int) -> list[NewEvent]:
                return [
                    NewEvent(
                        type="OrderCreated",
                        data=random_data(data_size),
                        content_type="application/octet-stream",
                    )
                    for _ in range(num_events)
                ]

            while True:
                print(num_events)
                events = generate_events(num_events)
                start = datetime.datetime.now()
                self.client.multi_append_to_stream(
                    events=NewEvents(
                        stream_name=stream_name1,
                        events=events,
                        current_version=StreamState.ANY,
                    ),
                )
                print(
                    "Rate:",
                    num_events / (datetime.datetime.now() - start).total_seconds(),
                    "events/s",
                )
                num_events *= 2

        self.assertGreater(cm.exception.size, data_size * num_events / 2)
        self.assertLess(cm.exception.max_size, cm.exception.size)

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_same_stream_error(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.

        with self.assertRaises(MultiAppendToSameStreamError) as cm:

            while True:
                self.client.multi_append_to_stream(
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

    # TODO: What gives StreamDeletedErrorDetails?

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_tombstoned_stream_error(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )
        self.client.multi_append_to_stream(
            events=NewEvents(
                stream_name=stream_name1,
                events=[event1],
                current_version=StreamState.NO_STREAM,
            ),
        )

        self.client.tombstone_stream(stream_name1, current_version=StreamState.EXISTS)

        with self.assertRaises(StreamTombstonedError):
            self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event1],
                    current_version=StreamState.ANY,
                ),
            )

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_metadata_conversions_and_errors(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        def append_helper(metadata: bytes) -> None:
            self.assertIsInstance(metadata, bytes)
            event = NewEvent(
                type="OrderCreated",
                data=random_data(),
                metadata=metadata,
                content_type="application/octet-stream",
            )
            self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name,
                    events=[event],
                    current_version=StreamState.ANY,
                ),
            )

        # These are OK.
        append_helper(b"")
        append_helper(json.dumps({"a": "1"}).encode())

        # These are not OK.
        with self.assertRaises(ProgrammingError):
            append_helper(random_data(100))
        with self.assertRaises(ProgrammingError):
            append_helper(json.dumps("a").encode())
        with self.assertRaises(ProgrammingError):
            append_helper(json.dumps({"a": 1}).encode())
        with self.assertRaises(ProgrammingError):
            append_helper(json.dumps({"a": {}}).encode())

        events = self.client.get_stream(stream_name)
        self.assertEqual(2, len(events))
        self.assertEqual(
            json.loads(events[0].metadata.decode()),
            {"$schema.format": "Bytes", "$schema.name": "OrderCreated"},
        )
        self.assertEqual(
            json.loads(events[1].metadata.decode()),
            {"$schema.format": "Bytes", "$schema.name": "OrderCreated", "a": "1"},
        )

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_stream_multi_append_deadline_exceeded(self) -> None:
        self.construct_esdb_client()
        stream_name1 = str(uuid4())

        # Construct a new event.
        event1 = NewEvent(
            type="OrderCreated",
            data=random_data(),
            content_type="application/octet-stream",
        )

        with self.assertRaises(GrpcDeadlineExceededError):
            self.client.multi_append_to_stream(
                events=NewEvents(
                    stream_name=stream_name1,
                    events=[event1],
                    current_version=StreamState.NO_STREAM,
                ),
                timeout=0,
            )


PROJECTION_QUERY_TEMPLATE1 = """fromStream('%s')
.when({
  $init: function(){
    return {
      count: 0
    };
  }
})
.outputState()
"""


class TestKurrentDBClientWithInsecureConnection(TestKurrentDBClient):
    KDB_TARGET = "localhost:2113"
    KDB_TLS = False

    @skip("Doesn't work with this class")
    def test_append_to_stream_wrong_credentials(self) -> None:
        super().test_append_to_stream_wrong_credentials()

    @skip("Doesn't work with this class")
    def test_stream_multi_append_wrong_credentials(self) -> None:
        super().test_stream_multi_append_wrong_credentials()

    @skip("Doesn't work with this class")
    def test_stream_multi_append_deadline_exceeded(self) -> None:
        super().test_stream_multi_append_deadline_exceeded()


# Todo: Test error from sending call credentials to insecure server
#  StatusCode.UNAUTHENTICATED
# 	details = "Established channel does not have a
# 	sufficient security level to transfer call credential."


class TestClusterNode1(TestKurrentDBClient):
    KDB_TARGET = "127.0.0.1:2110,127.0.0.1:2110"  # make it do discovery
    KDB_CLUSTER_SIZE = 3

    @skip("Doesn't work with this class")
    def test_stream_multi_append_deadline_exceeded(self) -> None:
        super().test_stream_multi_append_deadline_exceeded()


class TestClusterNode2(TestKurrentDBClient):
    KDB_TARGET = "127.0.0.1:2111,127.0.0.1:2111"  # make it do discovery
    KDB_CLUSTER_SIZE = 3

    @skip("Doesn't work with this class")
    def test_stream_multi_append_deadline_exceeded(self) -> None:
        super().test_stream_multi_append_deadline_exceeded()


class TestClusterNode3(TestKurrentDBClient):
    KDB_TARGET = "127.0.0.1:2112,127.0.0.1:2112"  # make it do discovery
    KDB_CLUSTER_SIZE = 3

    @skip("Doesn't work with this class")
    def test_stream_multi_append_deadline_exceeded(self) -> None:
        super().test_stream_multi_append_deadline_exceeded()


class TestRootCertificatesAreOptional(TimedTestCase):
    def test_tls_true_no_root_certificates(self) -> None:
        # NB Client can work with Tls=True without setting 'root_certificates'
        # if grpc lib can verify server cert using locally installed CA certs.
        uri = "kdb://admin:changeit@127.0.0.1:2110"
        client = KurrentDBClient(uri)
        with self.assertRaises(SSLError):
            client.get_commit_position()

    def test_one_target_tls_true_invalid_root_certificates(self) -> None:
        uri = "kdb://admin:changeit@127.0.0.1:2110"
        client = KurrentDBClient(uri, root_certificates="blah")

        with self.assertRaises(SSLError):
            client.get_commit_position()

        # # Todo: Wanted to do something like this:
        # with self.assertRaises(ServiceUnavailable) as cm:
        #     client.get_commit_position()
        # s = str(cm)
        # self.assertIn("Ssl handshake failed", s)
        # self.assertIn("routines:OPENSSL_internal:CERTIFICATE_VERIFY_FAILED", s)

    def test_two_targets_tls_true_invalid_root_certificates(self) -> None:
        uri = "kdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111"
        uri += "?MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

        with self.assertRaises(DiscoveryFailedError):
            KurrentDBClient(uri, root_certificates="blah")


class TestOptionalClientAuth(TimedTestCase):
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

    def test_tls_true_client_auth(self) -> None:
        secure_grpc_target = "localhost:2114"
        root_certificates = get_server_certificate(secure_grpc_target)
        uri = f"kdb://admin:changeit@{secure_grpc_target}"

        # Construct client without client auth.
        client = KurrentDBClient(uri, root_certificates=root_certificates)

        # User cert and key should be None.
        self.assertIsNone(client.private_key)
        self.assertIsNone(client.certificate_chain)

        # Should be able to get commit position.
        client.get_commit_position()

        # Construct client with client auth.
        uri += f"?UserKeyFile={self.user_key_file}&UserCertFile={self.user_cert_file}"
        client = KurrentDBClient(uri, root_certificates=root_certificates)

        # User cert and key should have expected values.
        self.assertEqual(self.user_key, client.private_key)
        self.assertEqual(self.user_cert, client.certificate_chain)

        # Should raise SSL error.
        with self.assertRaises(SSLError):
            client.get_commit_position()

        # Construct client with TlsCaFile (instead
        # of passing root_certificates directly).
        uri += f"&TlsCaFile={self.tls_ca_file}"
        client_with_tls_ca = KurrentDBClient(uri)

        # Read the contents of TlsCaFile as bytes,
        # because root_certificates are compared as bytes.
        with open(self.tls_ca_file, "rb") as f:
            tls_ca_file_contents = f.read()

        # TlsCaFile should override the root_certificates passed directly.
        self.assertNotEqual(root_certificates, client_with_tls_ca.root_certificates)
        self.assertEqual(tls_ca_file_contents, client_with_tls_ca.root_certificates)


class TestDiscoverScheme(TestCase):
    def test_calls_dns_and_uses_given_port_number_or_default(self) -> None:
        # Cluster name not configured in DNS, default port.
        uri = (
            "kdb+discover://my-unresolvable-cluster"
            "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=30"
        )
        with self.assertRaises(DiscoveryFailedError) as cm1:
            KurrentDBClient(uri)
        self.assertIn(":2113", str(cm1.exception))
        self.assertIn("DNS resolution failed", str(cm1.exception))
        self.assertNotIn("Deadline Exceeded", str(cm1.exception))

        # Cluster name not configured in DNS, non-default port.
        uri = (
            "kdb+discover://my-unresolvable-cluster:9898"
            "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=30"
        )
        with self.assertRaises(DiscoveryFailedError) as cm2:
            KurrentDBClient(uri)
        self.assertIn(":9898", str(cm2.exception))
        self.assertIn("DNS resolution failed", str(cm2.exception))
        self.assertNotIn("Deadline Exceeded", str(cm2.exception))

        # Name is resolvable but 'service not available' on port 2222.
        uri = "kdb://localhost:2222?Tls=false"
        client = KurrentDBClient(uri)
        with self.assertRaises(ServiceUnavailableError) as cm3:
            client.read_gossip()
        self.assertIn("Failed to connect to remote host", str(cm3.exception))

        uri = (
            "kdb+discover://localhost:2222"
            "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=30"
        )
        with self.assertRaises(DiscoveryFailedError) as cm4:
            KurrentDBClient(uri)
        self.assertIn(":2222", str(cm4.exception))
        self.assertIn("Failed to connect to remote host", str(cm4.exception))

        # # Name is resolvable but get no response from example.com:2222.
        # with self.assertRaises(GrpcDeadlineExceeded) as cm:
        #     uri = (
        #         "kdb://example.com:2222"
        #         "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=1"
        #     )
        #     client = KurrentDBClient(uri)
        #     client.read_gossip()
        # self.assertIn("Deadline Exceeded", str(cm.exception))
        # with self.assertRaises(DiscoveryFailed) as cm:
        #     uri = (
        #         "kdb+discover://example.com:2222"
        #         "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=1"
        #     )
        #     KurrentDBClient(uri)
        # self.assertIn(":2222", str(cm.exception))
        # self.assertIn("Deadline Exceeded", str(cm.exception))

        # # Name is resolvable but get no response from example.com:80.
        # with self.assertRaises(ServiceUnavailable) as cm:
        #     uri = (
        #         "kdb://example.com:80"
        #         "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=1"
        #     )
        #     with KurrentDBClient(uri) as client:
        #         client.read_gossip()
        # self.assertIn("No route to host", str(cm.exception))
        # # self.assertIn("Trying to connect an http1.x server", str(cm.exception))
        # with self.assertRaises(DiscoveryFailed) as cm:
        #     uri = (
        #         "kdb+discover://example.com:80"
        #         "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=1"
        #     )
        #     KurrentDBClient(uri)
        #
        # self.assertIn(":80", str(cm.exception))
        # # self.assertIn("No route to host", str(cm.exception))
        # self.assertIn("Trying to connect an http1.x server", str(cm.exception))

        # Discover insecure single-node cluster, connect to leader.
        uri = (
            "kdb+discover://localhost:2113"
            "?Tls=false&DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=30"
        )
        client = KurrentDBClient(uri)
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        client.append_events(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )
        self.assertEqual(len(client.get_stream(stream_name)), 2)

        # Discover insecure single-node cluster, but fail to connect to follower.
        with self.assertRaises(FollowerNotFoundError):
            KurrentDBClient(uri + "&NodePreference=follower")

        # Discover secure single-node cluster, connect to leader.
        uri = (
            "kdb+discover://admin:changeit@localhost:2114"
            "?DiscoveryInterval=0&MaxDiscoverAttempts=1&GossipTimeout=30"
        )
        root_certificates = get_server_certificate("localhost:2114")
        client = KurrentDBClient(uri, root_certificates=root_certificates)
        stream_name = str(uuid4())
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())
        client.append_events(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=[event1, event2],
        )
        self.assertEqual(len(client.get_stream(stream_name)), 2)

        # Discover secure single-node cluster, but fail to connect to follower.
        with self.assertRaises(FollowerNotFoundError):
            KurrentDBClient(
                uri + "&NodePreference=follower", root_certificates=root_certificates
            )

        # In three-node cluster, check at least one node is a follower (not a leader).
        root_certificates = get_ca_certificate()
        ports = ["2110", "2111", "2112"]
        with self.assertRaises(NodeIsNotLeaderError) as cm5:
            for port in ports:
                uri = (
                    f"kdb://admin:changeit@localhost:{port}?"
                    "DiscoveryInterval=0&MaxDiscoverAttempts=1&NodePreference=leader"
                )
                client = KurrentDBClient(uri, root_certificates=root_certificates)
                stream_name = str(uuid4())
                event1 = NewEvent(type="OrderCreated", data=random_data())
                event2 = NewEvent(type="OrderUpdated", data=random_data())
                # The follower will fail to append events, raising NodeIsNotLeader.
                client.append_events(
                    stream_name=stream_name,
                    current_version=StreamState.NO_STREAM,
                    events=[event1, event2],
                )
                self.assertEqual(len(client.get_stream(stream_name)), 2)
        self.assertIsNotNone(cm5.exception.host)
        self.assertIsNotNone(cm5.exception.port)

        # Discover three-node cluster, getting cluster info from each node in turn,
        # connect to the leader and connect to a follower, then write to the leader
        # and read from the follower. Should work for all nodes.
        for port in ports:
            uri = (
                f"kdb+discover://admin:changeit@localhost:{port}?"
                "DiscoveryInterval=0&MaxDiscoverAttempts=1"
            )
            leader = KurrentDBClient(
                uri + "&NodePreference=leader", root_certificates=root_certificates
            )
            follower = KurrentDBClient(
                uri + "&NodePreference=follower", root_certificates=root_certificates
            )
            # Write to leader.
            stream_name = str(uuid4())
            event1 = NewEvent(type="OrderCreated", data=random_data())
            event2 = NewEvent(type="OrderUpdated", data=random_data())
            leader.append_events(
                stream_name=stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event1, event2],
            )
            # Read from follower.
            self.assertEqual(len(follower.get_stream(stream_name)), 2)

    # Todo: Elsewhere, exercise code path where we try to connect to a secure server
    #  with Tls=false and insecure with Tls=true, and maybe wrap the errors better
    #  (probably 'socket closed' in description). This is different from getting
    #  the root certificate wrong, or failing to provide a root certificate.


class TestGrpcOptions(TestCase):
    def setUp(self) -> None:
        uri = (
            "kdb://localhost:2113"
            "?Tls=false&KeepAliveInterval=1234&KeepAliveTimeout=5678"
        )
        self.client = KurrentDBClient(uri)

    def tearDown(self) -> None:
        self.client.close()

    def test(self) -> None:
        options_dict = dict(self.client.grpc_options)
        self.assertEqual(
            options_dict["grpc.max_receive_message_length"],
            17 * 1024 * 1024,
        )
        self.assertEqual(options_dict["grpc.keepalive_ms"], 1234)
        self.assertEqual(options_dict["grpc.keepalive_timeout_ms"], 5678)


class TestRequiresLeaderHeader(TimedTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uri = "kdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111,127.0.0.1:2112"
        self.ca_cert = get_ca_certificate()
        self.writer = KurrentDBClient(
            self.uri + "?NodePreference=leader", root_certificates=self.ca_cert
        )
        self.reader = KurrentDBClient(
            self.uri + "?NodePreference=follower", root_certificates=self.ca_cert
        )

    def tearDown(self) -> None:
        try:
            for subscription in self.writer.list_subscriptions():
                self.writer.delete_subscription(
                    group_name=subscription.group_name,
                    stream_name=(
                        None
                        if subscription.event_source == "$all"
                        else subscription.event_source
                    ),
                )
            self.writer.close()
            self.reader.close()
        finally:
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
        old, self.writer._connection = self.writer._connection, self.reader._connection
        # - this is hopeful mitigation for the "Exception was thrown by handler"
        #   which is occasionally a cause of failure of test_append_events()
        #   with both KurrentDB 21.10.9 and 22.10.0.
        old.close()
        # sleep(0.1)
        sleep(1)

    def test_reconnects_to_new_leader_on_append_event(self) -> None:
        # Fail to write to follower.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeaderError):
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

        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.append_events(
                stream_name,
                current_version=StreamState.NO_STREAM,
                events=[event1, event2],
            )

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Todo: Occasionally getting "Exception was thrown by handler." from this. Why?
        #   kurrentdbclient.exceptions.ExceptionThrownByHandler:
        #   <_MultiThreadedRendezvous of RPC that terminated with:
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
            except ExceptionThrownByHandlerError:
                if retries == 0:
                    raise
                sleep(1)
            else:
                break

    @skipIf(SERVER_VERSION < (25, 1), "Doesn't support multi-append")
    def test_reconnects_to_new_leader_on_multi_append(self) -> None:
        # Fail to write to follower.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.multi_append_to_stream(
                NewEvents(
                    stream_name, current_version=StreamState.NO_STREAM, events=[event1]
                )
            )

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and write to leader.
        self.writer.multi_append_to_stream(
            NewEvents(
                stream_name, current_version=StreamState.NO_STREAM, events=[event1]
            )
        )

    def test_reconnects_to_new_leader_on_set_stream_metadata(self) -> None:
        # Fail to write to follower.
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeaderError):
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
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.delete_stream(stream_name, current_version=1)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Delete stream on leader.
        self.writer.delete_stream(stream_name, current_version=1)

    def test_reconnects_to_new_leader_on_tombstone_stream(self) -> None:
        # Fail to tombstone stream on follower.
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.tombstone_stream(str(uuid4()), current_version=StreamState.ANY)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Tombstone stream on leader.
        self.writer.tombstone_stream(str(uuid4()), current_version=StreamState.ANY)

    def test_reconnects_to_new_leader_on_create_subscription_to_all(self) -> None:
        # Fail to create subscription on follower.
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.create_subscription_to_all(group_name=f"group{uuid4()!s}")

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Create subscription on leader.
        self.writer.create_subscription_to_all(group_name=f"group{uuid4()!s}")

    def test_reconnects_to_new_leader_on_create_subscription_to_stream(self) -> None:
        # Fail to create subscription on follower.
        group_name = f"group{uuid4()!s}"
        stream_name = str(uuid4())
        with self.assertRaises(NodeIsNotLeaderError):
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
        group_name = f"group{uuid4()!s}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to read subscription on follower.
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.read_subscription_to_all(group_name=group_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and read subscription on leader.
        self.writer.read_subscription_to_all(group_name=group_name)

    def test_reconnects_to_new_leader_on_read_subscription_to_stream(self) -> None:
        # Create stream subscription on leader.
        group_name = f"group{uuid4()!s}"
        stream_name = str(uuid4())
        self.writer.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        # Fail to read stream subscription on follower.
        with self.assertRaises(NodeIsNotLeaderError):
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
        group_name = f"group{uuid4()!s}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to list subscriptions on follower.
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.list_subscriptions()

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and list subscriptions on leader.
        self.writer.list_subscriptions()

    def test_reconnects_to_new_leader_on_list_subscriptions_to_stream(self) -> None:
        # Create stream subscription on leader.
        group_name = f"group{uuid4()!s}"
        stream_name = str(uuid4())
        self.writer.create_subscription_to_stream(
            group_name=group_name, stream_name=stream_name
        )

        # Fail to list stream subscriptions on follower.
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.list_subscriptions_to_stream(stream_name=stream_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and list stream subscriptions on leader.
        self.writer.list_subscriptions_to_stream(stream_name=stream_name)

    def test_reconnects_to_new_leader_on_get_subscription_info(self) -> None:
        # Create subscription on leader.
        group_name = f"group{uuid4()!s}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to get subscription info on follower.
        with self.assertRaises(NodeIsNotLeaderError):
            self.reader.get_subscription_info(group_name=group_name)

        # Swap connection.
        self._set_reader_connection_on_writer()

        # Reconnect and get subscription info on leader.
        self.writer.get_subscription_info(group_name=group_name)

    def test_reconnects_to_new_leader_on_delete_subscription(self) -> None:
        # Create subscription on leader.
        group_name = f"group{uuid4()!s}"
        self.writer.create_subscription_to_all(group_name=group_name)

        # Fail to delete subscription on follower.
        with self.assertRaises(NodeIsNotLeaderError):
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
            except NotFoundError:
                pass
            else:
                if len(stream_events) == 2:
                    break
            sleep(0.1)

        # Change reader's node preference to 'leader'
        self.reader.connection_spec.options._node_preference = "leader"

        # Check reader reconnects to leader.
        self.assertNotEqual(
            self.reader._connection._grpc_target, self.writer._connection._grpc_target
        )
        connection_id = id(self.reader._connection)
        self.reader.get_stream(stream_name)
        self.assertNotEqual(connection_id, id(self.reader._connection))
        self.assertEqual(
            self.reader._connection._grpc_target, self.writer._connection._grpc_target
        )


class TestAutoReconnectClosedConnection(TimedTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uri = "kdb://admin:changeit@127.0.0.1:2110,127.0.0.1:2111,127.0.0.1:2112"
        self.ca_cert = get_ca_certificate()
        self.writer = KurrentDBClient(
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
        with self.assertRaises(NotFoundError):
            self.writer.get_stream(str(uuid4()))

    def test_read_subscription_to_all(self) -> None:
        # Read subscription - should reconnect.
        with self.assertRaises(NotFoundError):
            self.writer.read_subscription_to_all(str(uuid4()))


class TestAutoReconnectAfterServiceUnavailable(TimedTestCase):
    def setUp(self) -> None:
        super().setUp()
        uri = "kdb://admin:changeit@localhost:2114?MaxDiscoverAttempts=1&DiscoveryInterval=0"
        server_certificate = get_server_certificate("localhost:2114")
        self.client = KurrentDBClient(uri=uri, root_certificates=server_certificate)

        # Reconstruct connection with wrong port (to inspire ServiceUnavailable).
        self.client._connection.close()
        self.client._connection = self.client._construct_esdb_connection(
            "localhost:2222"
        )

    def tearDown(self) -> None:
        try:
            for subscription in self.client.list_subscriptions():
                self.client.delete_subscription(
                    group_name=subscription.group_name,
                    stream_name=(
                        None
                        if subscription.event_source == "$all"
                        else subscription.event_source
                    ),
                )
            self.client.close()
        finally:
            super().tearDown()

    def test_append_events(self) -> None:
        self.client.append_events(
            str(uuid4()),
            current_version=StreamState.NO_STREAM,
            events=[NewEvent(type="X", data=b"")],
        )

    def test_get_stream(self) -> None:
        with self.assertRaises(NotFoundError):
            self.client.get_stream(
                str(uuid4()),
            )

    def test_read_stream(self) -> None:
        read_response = self.client.read_stream(
            str(uuid4()),
        )
        with self.assertRaises(ServiceUnavailableError):
            tuple(read_response)

    def test_read_all(self) -> None:
        read_response = self.client.read_all()
        with self.assertRaises(ServiceUnavailableError):
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
        with self.assertRaises(NotFoundError):
            self.client.get_subscription_info(
                group_name=f"my-subscription-{uuid4().hex}"
            )

    def test_list_subscriptions(self) -> None:
        self.client.list_subscriptions()

    def test_list_subscriptions_to_stream(self) -> None:
        self.client.list_subscriptions_to_stream(stream_name=str(uuid4()))

    def test_delete_stream(self) -> None:
        with self.assertRaises(NotFoundError):
            self.client.delete_stream(
                stream_name=str(uuid4()), current_version=StreamState.NO_STREAM
            )

    def test_replay_parked_events(self) -> None:
        with self.assertRaises(NotFoundError):
            self.client.replay_parked_events(
                group_name=f"my-subscription-{uuid4().hex}"
            )

        with self.assertRaises(NotFoundError):
            self.client.replay_parked_events(
                group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
            )

    def test_delete_subscription(self) -> None:
        with self.assertRaises(NotFoundError):
            self.client.delete_subscription(group_name=f"my-subscription-{uuid4().hex}")

        with self.assertRaises(NotFoundError):
            self.client.delete_subscription(
                group_name=f"my-subscription-{uuid4().hex}", stream_name=str(uuid4())
            )

    def test_read_gossip(self) -> None:
        self.client.read_gossip()

    # Getting 'AccessDenied' with KurrentDB v23.10.
    # def test_read_cluster_gossip(self) -> None:
    #     self.client.read_cluster_gossip()


class TestRaisesDiscoveryFailed(KurrentDBClientTestCase):
    KDB_TARGET = "localhost:2222,localhost:2222"  # make it do discovery
    KDB_TLS = False

    def test(self) -> None:
        with self.assertRaises(DiscoveryFailedError):
            self.construct_esdb_client()


class TestConnectsDespiteBadTarget(KurrentDBClientTestCase):
    KDB_TARGET = "localhost:2222,localhost:2113"  # make it do discovery
    KDB_TLS = False

    def test(self) -> None:
        self.construct_esdb_client()
        self.client.get_commit_position()
        self.assertEqual("localhost:2113", self.client.connection_target)


class TestConnectToPreferredNode(KurrentDBClientTestCase):
    KDB_TARGET = "localhost:2114,localhost:2114"  # make it do discovery
    KDB_CLUSTER_SIZE = 1

    def test_no_followers(self) -> None:
        with self.assertRaises(FollowerNotFoundError):
            self.construct_esdb_client("NodePreference=follower")

    def test_no_read_only_replicas(self) -> None:
        with self.assertRaises(ReadOnlyReplicaNotFoundError):
            self.construct_esdb_client("NodePreference=readonlyreplica")

    def test_random(self) -> None:
        self.construct_esdb_client("NodePreference=random")


class TestSubscriptionReadRequest(TimedTestCase):
    def test_request_ack_after_100_acks(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        self.assertEqual(read_request._max_ack_batch_size, 50)

        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        event_ids: list[UUID] = []
        for _ in range(102):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.ack(event_id)
        sleep(read_request._max_ack_delay)
        grpc_read_req1 = next(read_request)
        grpc_read_req2 = next(read_request)
        grpc_read_req3 = next(read_request)
        self.assertIsInstance(grpc_read_req1, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req1.ack.ids), 50)
        self.assertEqual(len(grpc_read_req1.nack.ids), 0)
        self.assertIsInstance(grpc_read_req2, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req2.ack.ids), 50)
        self.assertEqual(len(grpc_read_req2.nack.ids), 0)
        self.assertIsInstance(grpc_read_req3, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req3.ack.ids), 2)
        self.assertEqual(len(grpc_read_req3.nack.ids), 0)

    def test_request_nack_after_100_nacks(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        self.assertEqual(read_request._max_ack_batch_size, 50)

        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        event_ids: list[UUID] = []
        for _ in range(102):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.nack(event_id, "park")
        sleep(read_request._max_ack_delay)
        grpc_read_req1 = next(read_request)
        grpc_read_req2 = next(read_request)
        grpc_read_req3 = next(read_request)
        self.assertIsInstance(grpc_read_req1, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req1.nack.ids), 50)
        self.assertIsInstance(grpc_read_req2, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 50)
        self.assertIsInstance(grpc_read_req3, persistent_pb2.ReadReq)
        self.assertEqual(len(grpc_read_req3.ack.ids), 0)
        self.assertEqual(len(grpc_read_req3.nack.ids), 2)

    def test_request_ack_ack_ack(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        read_request_iter = read_request
        grpc_read_req1 = next(read_request_iter)
        self.assertIsInstance(grpc_read_req1, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req1.options.buffer_size, 150)

        # Do three acks.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        grpc_read_req2 = next(read_request_iter)
        read_request.ack(event_id2)
        read_request.ack(event_id3)
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
        grpc_read_options = next(read_request_iter)
        self.assertIsInstance(grpc_read_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_options.options.buffer_size, 150)

        # Do three nack unknown.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "unknown")
        grpc_read_req1 = next(read_request_iter)
        read_request.nack(event_id2, "unknown")
        read_request.nack(event_id3, "unknown")
        grpc_read_req2 = next(read_request_iter)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 2)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req2.nack.ids[1].string, str(event_id3))
        self.assertEqual(
            grpc_read_req1.nack.action, persistent_pb2.ReadReq.Nack.Unknown
        )
        self.assertEqual(
            grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Unknown
        )

    def test_request_nack_park_after_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        grpc_read_req1 = next(read_request)
        read_request.nack(event_id2, "park")
        read_request.nack(event_id3, "park")
        grpc_read_req2 = next(read_request)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 2)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req2.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req1.nack.action, persistent_pb2.ReadReq.Nack.Park)
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Park)

    def test_request_nack_retry_after_max_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "retry")
        grpc_read_req1 = next(read_request)
        read_request.nack(event_id2, "retry")
        read_request.nack(event_id3, "retry")
        grpc_read_req2 = next(read_request)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 2)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req2.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req1.nack.action, persistent_pb2.ReadReq.Nack.Retry)
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Retry)

    def test_request_nack_skip_after_100ms(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "skip")
        grpc_read_req1 = next(read_request)
        read_request.nack(event_id2, "skip")
        read_request.nack(event_id3, "skip")
        sleep(read_request._max_ack_delay)
        grpc_read_req2 = next(read_request)
        self.assertEqual(len(grpc_read_req1.nack.ids), 1)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 2)
        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id2))
        self.assertEqual(grpc_read_req2.nack.ids[1].string, str(event_id3))
        self.assertEqual(grpc_read_req1.nack.action, persistent_pb2.ReadReq.Nack.Skip)
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Skip)

    def test_request_nack_stop_after_max_delay(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        grpc_read_options = next(read_request)
        self.assertIsInstance(grpc_read_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_options.options.buffer_size, 150)

        # Do three nack park.
        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "stop")
        grpc_read_req2 = next(read_request)
        read_request.nack(event_id2, "stop")
        read_request.nack(event_id3, "stop")
        grpc_read_req3 = next(read_request)
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
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        grpc_read_req1 = next(read_request)
        read_request.ack(event_id2)
        read_request.nack(event_id3, "park")

        grpc_read_req2 = next(read_request)
        grpc_read_req3 = next(read_request)
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
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        read_request.nack(event_id2, "park")
        read_request.ack(event_id3)

        grpc_read_req1 = next(read_request)
        grpc_read_req2 = next(read_request)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req1.nack.ids), 2)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req1.nack.ids[1].string, str(event_id2))

        self.assertEqual(len(grpc_read_req2.ack.ids), 1)
        self.assertEqual(len(grpc_read_req2.nack.ids), 0)
        self.assertEqual(grpc_read_req2.ack.ids[0].string, str(event_id3))

    def test_request_nack_after_nack_followed_by_nack_with_other_action(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.nack(event_id1, "park")
        read_request.nack(event_id2, "park")
        read_request.nack(event_id3, "skip")

        grpc_read_req1 = next(read_request)
        grpc_read_req2 = next(read_request)
        self.assertEqual(len(grpc_read_req1.ack.ids), 0)
        self.assertEqual(len(grpc_read_req1.nack.ids), 2)
        self.assertEqual(grpc_read_req1.nack.ids[0].string, str(event_id1))
        self.assertEqual(grpc_read_req1.nack.ids[1].string, str(event_id2))
        self.assertEqual(grpc_read_req1.nack.action, persistent_pb2.ReadReq.Nack.Park)

        self.assertEqual(len(grpc_read_req2.ack.ids), 0)
        self.assertEqual(len(grpc_read_req2.nack.ids), 1)
        self.assertEqual(grpc_read_req2.nack.ids[0].string, str(event_id3))
        self.assertEqual(grpc_read_req2.nack.action, persistent_pb2.ReadReq.Nack.Skip)

    def test_request_iter_stop(self) -> None:
        read_request = SubscriptionReadReqs("group1")
        grpc_read_req_options = next(read_request)
        self.assertIsInstance(grpc_read_req_options, grpc_persistent.ReadReq)
        self.assertEqual(grpc_read_req_options.options.buffer_size, 150)

        event_id1 = uuid4()
        event_id2 = uuid4()
        event_id3 = uuid4()
        read_request.ack(event_id1)
        read_request.ack(event_id2)
        read_request.ack(event_id3)
        thread = Thread(target=read_request.stop)
        thread.start()

        sleep(read_request._max_ack_delay)

        try:
            while True:
                grpc_read_req1 = next(read_request)
        except StopIteration:
            pass

        thread.join()

        self.assertEqual(len(grpc_read_req1.ack.ids), 3)
        self.assertEqual(len(grpc_read_req1.nack.ids), 0)


class TestHandleRpcError(TestCase):
    def test_handle_exception_thrown_by_handler(self) -> None:
        with self.assertRaises(ExceptionThrownByHandlerError):
            raise handle_rpc_error(FakeExceptionThrownByHandlerError()) from None

    def test_handle_deadline_exceeded_error(self) -> None:
        with self.assertRaises(GrpcDeadlineExceededError):
            raise handle_rpc_error(FakeDeadlineExceededRpcError()) from None

    def test_handle_unavailable_error(self) -> None:
        with self.assertRaises(ServiceUnavailableError):
            raise handle_rpc_error(FakeUnavailableRpcError()) from None

    def test_handle_not_leader_node_error(self) -> None:
        with self.assertRaises(NodeIsNotLeaderError) as cm:
            raise handle_rpc_error(FakeNotLeaderNodeRpcError()) from None

        # Check the trailing metadata is parsed correctly.
        self.assertEqual(cm.exception.host, "127.0.0.1")
        self.assertEqual(cm.exception.port, 2111)
        self.assertIsNone(cm.exception.node_id)

    def test_handle_not_leader_node_v2_error(self) -> None:
        leader_host = "127.0.0.1"
        leader_port = 2111
        node_id = str(uuid4())
        with self.assertRaises(NodeIsNotLeaderError) as cm:
            raise handle_rpc_error(
                FakeNotLeaderNodeV2RpcError(
                    host=leader_host,
                    port=leader_port,
                    node_id=node_id,
                )
            ) from None

        # Check the status details are unpacked correctly.
        self.assertEqual(cm.exception.host, leader_host)
        self.assertEqual(cm.exception.port, leader_port)
        self.assertEqual(cm.exception.node_id, node_id)

    def test_handle_stream_revision_conflict_error_v2(self) -> None:
        stream_name = str(uuid4())
        expected_version = 0
        current_version = -1
        with self.assertRaises(WrongCurrentVersionError) as cm:
            raise handle_rpc_error(
                FakeStreamRevisionConflictV2RpcError(
                    stream_name,
                    expected_revision=expected_version,
                    actual_revision=current_version,
                )
            ) from None

        self.assertEqual(stream_name, cm.exception.stream_name)
        self.assertEqual(expected_version, cm.exception.expected_version)
        self.assertEqual(current_version, cm.exception.current_version)

    def test_handle_stream_already_in_append_session_error_v2(self) -> None:
        stream_name = str(uuid4())
        with self.assertRaises(MultiAppendToSameStreamError) as cm:
            raise handle_rpc_error(
                FakeStreamAlreadyInAppendSessionV2RpcError(
                    stream_name,
                )
            ) from None

        self.assertEqual(stream_name, cm.exception.stream_name)

    def test_handle_stream_tombstoned_error_v2(self) -> None:
        stream_name = str(uuid4())
        with self.assertRaises(StreamTombstonedError) as cm:
            raise handle_rpc_error(
                FakeStreamTombstonedV2RpcError(stream=stream_name)
            ) from None

        self.assertEqual(stream_name, cm.exception.stream_name)

    def test_handle_append_record_size_exceeded_error_v2(self) -> None:
        stream_name = str(uuid4())
        event_id = uuid4()
        size = 100
        max_size = 50
        with self.assertRaises(RecordMaxSizeExceededError) as cm:
            raise handle_rpc_error(
                FakeAppendRecordSizeExceededV2RpcError(
                    stream=stream_name,
                    record_id=str(event_id),
                    size=size,
                    max_size=max_size,
                )
            ) from None

        self.assertEqual(stream_name, cm.exception.stream_name)
        self.assertEqual(event_id, cm.exception.event_id)
        self.assertEqual(size, cm.exception.size)
        self.assertEqual(max_size, cm.exception.max_size)

    def test_handle_append_transaction_size_exceeded_error_v2(self) -> None:
        size = 100
        max_size = 50
        with self.assertRaises(TransactionMaxSizeExceededError) as cm:
            raise handle_rpc_error(
                FakeAppendTransactionSizeExceededV2RpcError(
                    size=size, max_size=max_size
                )
            ) from None

        self.assertEqual(size, cm.exception.size)
        self.assertEqual(max_size, cm.exception.max_size)

    def test_handle_consumer_too_slow_error(self) -> None:
        with self.assertRaises(ConsumerTooSlowError):
            raise handle_rpc_error(FakeConsumerTooSlowError()) from None

    def test_handle_aborted_by_server_error(self) -> None:
        with self.assertRaises(AbortedByServerError):
            raise handle_rpc_error(FakeAbortedByServerError()) from None

    def test_handle_unknown_error(self) -> None:
        with self.assertRaises(UnknownError) as cm:
            raise handle_rpc_error(FakeUnknownRpcError()) from None
        self.assertEqual(UnknownError, cm.exception.__class__)

    def test_handle_non_call_rpc_error(self) -> None:
        # Check non-Call errors are handled.
        class MyRpcError(RpcError):
            pass

        my_rpc_error = MyRpcError("some non-Call error")
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(my_rpc_error) from None
        self.assertEqual(cm.exception.__class__, GrpcError)
        self.assertEqual(cm.exception.args[0], str(my_rpc_error))

    def test_streams_unknown_error(self) -> None:
        with self.assertRaises(UnknownError):
            raise handle_streams_rpc_error(FakeUnknownRpcError()) from None

    def test_stream_not_found_error(self) -> None:
        with self.assertRaises(NotFoundError):
            raise handle_streams_rpc_error(
                FakeWrongExpectedVersionActuallyMinusOneError()
            ) from None

    def test_stream_wrong_version_error(self) -> None:
        with self.assertRaises(WrongCurrentVersionError):
            raise handle_streams_rpc_error(
                FakeWrongExpectedVersionActuallyPlusOneError()
            ) from None

    def test_stream_deleted_error(self) -> None:
        with self.assertRaises(StreamIsDeletedError):
            raise handle_streams_rpc_error(FakeStreamIsDeletedError()) from None

    def test_stream_other_failed_precondition_error(self) -> None:
        with self.assertRaises(FailedPreconditionError):
            raise handle_streams_rpc_error(FakeFailedPreconditionRpcError()) from None

    def test_handle_unsupported_v2_error(self) -> None:
        with self.assertRaises(KurrentDBClientError) as cm:
            raise handle_rpc_error(FakeUnsupportedV2RpcError()) from None
        self.assertEqual(KurrentDBClientError, cm.exception.__class__)
        self.assertEqual("Actually OK", cm.exception.args[0])


class FakeRpcError(_MultiThreadedRendezvous):
    def __init__(
        self,
        status_code: StatusCode,
        details: str = "",
        trailing_metadata: list[tuple[str, str | bytes]] | None = None,
    ) -> None:
        super().__init__(
            state=_RPCState(
                due=[],
                initial_metadata=None,
                trailing_metadata=trailing_metadata,
                code=status_code,
                details=details,
            ),
            call=IntegratedCall(None, None),
            response_deserializer=lambda x: x,
            deadline=None,
        )

    def trailing_metadata(self) -> Any:
        return self._state.trailing_metadata


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


class FakeNotLeaderNodeRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(
            status_code=StatusCode.NOT_FOUND,
            details="Leader info available",
            trailing_metadata=[
                ("leader-endpoint-host", "127.0.0.1"),
                ("leader-endpoint-port", "2111"),
            ],
        )


class FakeNotLeaderNodeV2RpcError(FakeRpcError):
    def __init__(self, host: str, port: int, node_id: str) -> None:
        message = (
            "The server is not the leader node and cannot handle "
            "the request. Please retry your request against the "
            f"leader node directly at {host}:{port}"
        )
        super().__init__(
            status_code=StatusCode.FAILED_PRECONDITION,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_not_leader_node_error_details_v2(
                        message=message,
                        host="127.0.0.1",
                        port=2111,
                        node_id=node_id,
                    ).SerializeToString(),
                ),
            ],
        )


class FakeStreamRevisionConflictV2RpcError(FakeRpcError):
    def __init__(
        self, stream: str, expected_revision: int, actual_revision: int
    ) -> None:
        message = (
            f"Append failed due to a revision conflict on stream '{stream}'."
            f" Expected revision: {expected_revision}."
            f" Actual revision: {actual_revision}."
        )
        super().__init__(
            status_code=StatusCode.FAILED_PRECONDITION,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_stream_revision_conflict_error_details_v2(
                        message=message,
                        stream=stream,
                        expected_revision=expected_revision,
                        actual_revision=actual_revision,
                    ).SerializeToString(),
                ),
            ],
        )


class FakeStreamAlreadyInAppendSessionV2RpcError(FakeRpcError):
    def __init__(self, stream: str) -> None:
        message = (
            f"Stream '{stream}' is already part of this append session."
            f" Appending to the same stream multiple times is currently"
            f" not supported."
        )
        super().__init__(
            status_code=StatusCode.ABORTED,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_stream_already_in_append_session_error_details_v2(
                        message=message,
                        stream=stream,
                    ).SerializeToString(),
                ),
            ],
        )


class FakeStreamTombstonedV2RpcError(FakeRpcError):
    def __init__(self, stream: str) -> None:
        message = (
            f"Stream '{stream}' has been tombstoned. It has been"
            f" permanently removed from the system and cannot be restored."
        )
        super().__init__(
            status_code=StatusCode.FAILED_PRECONDITION,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_stream_tombstoned_error_details_v2(
                        message=message,
                        stream=stream,
                    ).SerializeToString(),
                ),
            ],
        )


class FakeAppendRecordSizeExceededV2RpcError(FakeRpcError):
    def __init__(self, stream: str, record_id: str, size: int, max_size: int) -> None:
        size_mb = size / (1024 * 1014)
        max_size_mb = max_size / (1024 * 1014)
        diff_mb = size_mb - max_size_mb
        message = (
            f"'The size of record {record_id} ({size_mb:.3f} MB) exceeds the maximum"
            f" allowed size of {max_size_mb:.3f} MB bytes by {diff_mb:.3} MB'"
        )
        super().__init__(
            status_code=StatusCode.INVALID_ARGUMENT,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_append_record_size_exceeded_error_details_v2(
                        message=message,
                        stream=stream,
                        record_id=record_id,
                        size=size,
                        max_size=max_size,
                    ).SerializeToString(),
                ),
            ],
        )


class FakeAppendTransactionSizeExceededV2RpcError(FakeRpcError):
    def __init__(self, size: int, max_size: int) -> None:
        size_mb = size / (1024 * 1014)
        max_size_mb = max_size / (1024 * 1014)
        diff_mb = size_mb - max_size_mb
        message = (
            f"Transaction size ({size_mb:.3f} MB) exceeded"
            f" the maximum allowed size of {max_size_mb:.3f} MB"
            f" by {diff_mb:.3f} MB, after 26 record(s)."
        )
        super().__init__(
            status_code=StatusCode.ABORTED,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_append_transaction_size_exceeded_error_details_v2(
                        message=message,
                        size=size,
                        max_size=max_size,
                    ).SerializeToString(),
                ),
            ],
        )


class FakeUnsupportedV2RpcError(FakeRpcError):
    def __init__(self) -> None:
        message = "Actually OK"
        super().__init__(
            status_code=StatusCode.OK,
            details=message,
            trailing_metadata=[
                (
                    "grpc-status-details-bin",
                    status_with_some_unsupported_error_details_v2(
                        message=message,
                    ).SerializeToString(),
                ),
            ],
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


class FakeFailedPreconditionRpcError(FakeRpcError):
    def __init__(self, details: str = "") -> None:
        super().__init__(status_code=StatusCode.FAILED_PRECONDITION, details=details)


class FakeWrongExpectedVersionActuallyMinusOneError(FakeFailedPreconditionRpcError):
    def __init__(self) -> None:
        super().__init__(details="WrongExpectedVersion Actual version: -1")


class FakeWrongExpectedVersionActuallyPlusOneError(FakeFailedPreconditionRpcError):
    def __init__(self) -> None:
        super().__init__(details="WrongExpectedVersion Actual version: 1")


class FakeStreamIsDeletedError(FakeFailedPreconditionRpcError):
    def __init__(self) -> None:
        super().__init__(details="is deleted")


def random_data(size: int = 16) -> bytes:
    return os.urandom(size)


# del KurrentDBClientTestCase
