# -*- coding: utf-8 -*-
import os
import ssl
from time import sleep
from typing import List
from unittest import TestCase
from uuid import UUID, uuid4

from grpc import RpcError, StatusCode
from grpc._channel import _MultiThreadedRendezvous, _RPCState
from grpc._cython.cygrpc import IntegratedCall

import esdbclient.protos.Grpc.persistent_pb2 as grpc_persistent
from esdbclient.client import ESDBClient
from esdbclient.esdbapi import SubscriptionReadRequest, handle_rpc_error
from esdbclient.events import NewEvent
from esdbclient.exceptions import (
    DeadlineExceeded,
    ExpectedPositionError,
    GrpcError,
    ServiceUnavailable,
    StreamDeletedError,
    StreamNotFound,
)


class FakeRpcError(_MultiThreadedRendezvous):
    def __init__(self, status_code: StatusCode) -> None:
        super().__init__(
            state=_RPCState(
                due=[],
                initial_metadata=None,
                trailing_metadata=None,
                code=status_code,
                details="",
            ),
            call=IntegratedCall(None, None),
            response_deserializer=lambda x: x,
            deadline=None,
        )


class FakeDeadlineExceededRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.DEADLINE_EXCEEDED)


class FakeUnavailableRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.UNAVAILABLE)


class FakeUnknownRpcError(FakeRpcError):
    def __init__(self) -> None:
        super().__init__(status_code=StatusCode.UNKNOWN)


class TestESDBClient(TestCase):
    client: ESDBClient

    def tearDown(self) -> None:
        if hasattr(self, "client"):
            self.client.close()

    def test_close(self) -> None:
        self.construct_esdb_client()
        self.client.close()
        self.client.close()

        self.client = ESDBClient("localhost:2222")
        self.client.close()
        self.client.close()

    def test_constructor_args(self) -> None:
        client1 = ESDBClient("localhost:2222")
        self.assertEqual(client1.grpc_target, "localhost:2222")

        client2 = ESDBClient(host="localhost", port=2222)
        self.assertEqual(client2.grpc_target, "localhost:2222")

        # ESDB URLs not yet supported...
        with self.assertRaises(ValueError):
            ESDBClient(uri="esdb:something")

    def test_service_unavailable_exception(self) -> None:
        self.client = ESDBClient("localhost:2222")  # wrong port

        with self.assertRaises(ServiceUnavailable) as cm:
            list(self.client.read_stream_events(str(uuid4())))
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        with self.assertRaises(ServiceUnavailable) as cm:
            self.client.append_events(str(uuid4()), expected_position=None, events=[])
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        with self.assertRaises(ServiceUnavailable) as cm:
            self.client.append_event(
                str(uuid4()), expected_position=None, event=NewEvent(type="", data=b"")
            )
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        with self.assertRaises(ServiceUnavailable) as cm:
            group_name = f"my-subscription-{uuid4().hex}"
            self.client.create_subscription(
                group_name=group_name,
                filter_include=["OrderCreated"],
            )
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

        with self.assertRaises(ServiceUnavailable) as cm:
            group_name = f"my-subscription-{uuid4().hex}"
            read_req, read_resp = self.client.read_subscription(
                group_name=group_name,
            )
            list(read_resp)
        self.assertIn(
            "failed to connect to all addresses", cm.exception.args[0].details()
        )

    def test_handle_deadline_exceeded_error(self) -> None:
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(FakeDeadlineExceededRpcError()) from None
        self.assertEqual(cm.exception.__class__, DeadlineExceeded)

    def test_handle_unavailable_error(self) -> None:
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(FakeUnavailableRpcError()) from None
        self.assertEqual(cm.exception.__class__, ServiceUnavailable)

    def test_handle_other_call_error(self) -> None:
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(FakeUnknownRpcError()) from None
        self.assertEqual(cm.exception.__class__, GrpcError)

    def test_handle_non_call_rpc_error(self) -> None:
        # Check non-Call errors are handled.
        class MyRpcError(RpcError):
            pass

        msg = "some non-Call error"
        with self.assertRaises(GrpcError) as cm:
            raise handle_rpc_error(MyRpcError(msg)) from None
        self.assertEqual(cm.exception.__class__, GrpcError)
        self.assertIsInstance(cm.exception.args[0], MyRpcError)

    def construct_esdb_client(self) -> None:
        server_cert = ssl.get_server_certificate(addr=("localhost", 2113))
        username = "admin"
        password = "changeit"
        self.client = ESDBClient(
            host="localhost",
            port=2113,
            server_cert=server_cert,
            username=username,
            password=password,
        )

    def test_stream_not_found_exception(self) -> None:
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
        with self.assertRaises(ExpectedPositionError) as cm:
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

        with self.assertRaises(ExpectedPositionError) as cm:
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
        with self.assertRaises(ExpectedPositionError) as cm:
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

    def test_append_events_multiplexed_without_occ(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append batch of new events.
        commit_position2 = self.client.append_events_multiplexed(
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
        commit_position4 = self.client.append_events_multiplexed(
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

    def test_append_events_multiplexed_with_occ(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        commit_position0 = self.client.get_commit_position()

        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Fail to append (stream does not exist).
        with self.assertRaises(StreamNotFound):
            self.client.append_events_multiplexed(
                stream_name, expected_position=1, events=[event1, event2]
            )

        # Append batch of new events.
        commit_position2 = self.client.append_events_multiplexed(
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
        with self.assertRaises(ExpectedPositionError):
            self.client.append_events_multiplexed(
                stream_name, expected_position=None, events=[event3, event4]
            )

        # Fail to append (wrong expected position).
        with self.assertRaises(ExpectedPositionError):
            self.client.append_events_multiplexed(
                stream_name, expected_position=10, events=[event3, event4]
            )

        # Read stream and check recorded events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, event1.id)
        self.assertEqual(events[1].id, event2.id)

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
        with self.assertRaises(ExpectedPositionError):
            self.client.append_events(
                stream_name, expected_position=None, events=[event3, event4]
            )

        # Fail to append (wrong expected position).
        with self.assertRaises(ExpectedPositionError):
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

    def test_read_all_events(self) -> None:
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

    def test_timeout_read_all_events(self) -> None:
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

    def test_read_all_filter_include(self) -> None:
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

    def test_read_all_filter_exclude(self) -> None:
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

    def test_read_all_filter_exclude_ignored_when_filter_include_is_set(self) -> None:
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

    def test_catchup_subscribe_all_events_default_filter(self) -> None:
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

    def test_catchup_subscribe_stream_events_default_filter(self) -> None:
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

    def test_catchup_subscribe_stream_events_from_stream_position(self) -> None:
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

    def test_catchup_subscribe_all_events_no_filter(self) -> None:
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

    def test_catchup_subscribe_all_events_include_filter(self) -> None:
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
            filter_exclude=[],
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

    def test_catchup_subscribe_all_events_from_commit_position_zero(self) -> None:
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

    def test_catchup_subscribe_all_events_from_commit_position_current(self) -> None:
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

    def test_persistent_subscription_from_start(self) -> None:
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

    def test_persistent_subscription_from_commit_position(self) -> None:
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

    def test_persistent_subscription_from_end(self) -> None:
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
            # print(event.type)
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

    def test_persistent_subscription_include_filter(self) -> None:
        self.construct_esdb_client()

        # Create persistent subscription.
        group_name = f"my-subscription-{uuid4().hex}"
        self.client.create_subscription(
            group_name=group_name,
            filter_include=["OrderCreated"],
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

        for event in read_resp:
            read_req.ack(event.id)
            self.assertEqual(event.type, "OrderCreated")
            if event.data == event1.data:
                break

    def test_persistent_subscription_exclude_filter(self) -> None:
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

        # start = datetime.now()
        # absstart = start
        for _, event in enumerate(read_resp):
            # received = datetime.now()
            # duration = (received - start).total_seconds()
            # total_duration = (received - absstart).total_seconds()
            # rate = i / total_duration
            # start = received
            read_req.ack(event.id)
            # print(i, f"duration: {duration:.4f}s", f"rate: {rate:.1f}/s --", event)
            # if duration > 0.5:
            #     print("^^^^^^^^^^^^^^^^^^^^^^^^ seemed to take a long time")
            #     print()
            self.assertNotEqual(event.type, "OrderCreated")
            if event.data == event3.data:
                break

    def test_persistent_subscription_no_filter(self) -> None:
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

    def test_persistent_stream_subscription_from_start(self) -> None:
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

    def test_persistent_stream_subscription_from_stream_position(self) -> None:
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

    def test_persistent_stream_subscription_from_end(self) -> None:
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

    # Todo: "commit position" behaviour (not sure why it isn't working)
    # Todo: consumer_strategy, RoundRobin and Pinned, need to test with more than
    #  one consumer, also code this as enum rather than a string
    # Todo: Nack? exception handling on callback?
    # Todo: update subscription
    # Todo: delete subscription
    # Todo: filter options
    # Todo: subscribe from end? not interesting, because you can get commit position

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

        # Append two events.
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        with self.assertRaises(GrpcError) as cm:
            self.client.delete_stream(stream_name, expected_position=0)
        # Todo: Maybe can extract this better?
        self.assertIn("WrongExpectedVersion", str(cm.exception))

        # Delete the stream, specifying expected position.
        self.client.delete_stream(stream_name, expected_position=1)

        # Can call delete again, without error.
        self.client.delete_stream(stream_name, expected_position=1)

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Expect stream position is None.
        # Todo: Then how to you know which expected_position to use
        #  when subsequently appending?
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can append to a deleted stream.
        with self.assertRaises(ExpectedPositionError):
            self.client.append_events(stream_name, expected_position=0, events=[event3])
        self.client.append_events(stream_name, expected_position=1, events=[event3])

        # Can read from deleted stream if new events have been appended.
        # Todo: This behaviour is a little bit flakey? Sometimes we get StreamNotFound.
        sleep(0.1)
        events = list(self.client.read_stream_events(stream_name))
        # Expect only to get events appended after stream was deleted.
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event3.id)

        # Delete the stream again, specifying expected position.
        with self.assertRaises(GrpcError) as cm:
            self.client.delete_stream(stream_name, expected_position=0)
        # Todo: Maybe can extract this better?
        self.assertIn("WrongExpectedVersion", str(cm.exception))

        self.client.delete_stream(stream_name, expected_position=2)
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can delete again without error.
        self.client.delete_stream(stream_name, expected_position=2)

    def test_delete_stream_with_any_expected_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Can't delete stream that doesn't exist, while expecting "any" version.
        # Todo: I don't really understand why this should cause an error.
        with self.assertRaises(GrpcError) as cm:
            self.client.delete_stream(stream_name, expected_position=-1)
        self.assertIn("Actual version: -1", str(cm.exception))

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
        with self.assertRaises(ExpectedPositionError):
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

        # Can't delete stream that doesn't exist, while expecting that it does exist.
        with self.assertRaises(GrpcError) as cm:
            self.client.delete_stream(stream_name, expected_position=None)
        self.assertIn("Actual version: -1", str(cm.exception))

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

        # Can't call delete again, because stream doesn't exist.
        with self.assertRaises(GrpcError) as cm:
            self.client.delete_stream(stream_name, expected_position=None)
        self.assertIn("is deleted", str(cm.exception))

        # Expect "stream not found" when reading deleted stream.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Expect stream position is None.
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can append to a deleted stream.
        with self.assertRaises(ExpectedPositionError):
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
        self.client.delete_stream(stream_name, expected_position=None)
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))
        self.assertEqual(None, self.client.get_stream_position(stream_name))

        # Can't call delete again, because stream doesn't exist.
        with self.assertRaises(GrpcError) as cm:
            self.client.delete_stream(stream_name, expected_position=None)
        self.assertIn("is deleted", str(cm.exception))

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

        with self.assertRaises(GrpcError) as cm:
            self.client.tombstone_stream(stream_name, expected_position=0)
        # Todo: Maybe can extract this better?
        self.assertIn("WrongExpectedVersion", str(cm.exception))

        # Tombstone the stream, specifying expected position.
        self.client.tombstone_stream(stream_name, expected_position=1)

        # Can't call tombstone again.
        with self.assertRaises(GrpcError) as cm:
            self.client.tombstone_stream(stream_name, expected_position=1)
        self.assertIn("is deleted", str(cm.exception))

        # Expect "stream not found" when reading tombstoned stream.
        # with self.assertRaises(StreamNotFound):
        #     list(self.client.read_stream_events(stream_name))
        # Actually we get a message saying the stream is deleted.
        with self.assertRaises(GrpcError) as cm:
            list(self.client.read_stream_events(stream_name))
        self.assertIn("is deleted", str(cm.exception))

        # Expect stream position is None.
        # Todo: Then how to you know which expected_position to use
        #  when subsequently appending?
        # self.assertEqual(None, self.client.get_stream_position(stream_name))
        # Actually we get a message saying the stream is deleted.
        with self.assertRaises(GrpcError) as cm:
            self.client.get_stream_position(stream_name)
        self.assertIn("is deleted", str(cm.exception))

        # Can't append to a tombstoned stream.
        with self.assertRaises(StreamDeletedError):
            self.client.append_events(stream_name, expected_position=1, events=[event3])

    def test_tombstone_stream_with_any_expected_position(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Can tombstone stream that doesn't exist, while expecting "any" version.
        # Todo: I don't really understand why this shouldn't cause an error,
        #  if we can't do the same with the delete operation.
        self.client.tombstone_stream(stream_name, expected_position=-1)

        # Construct two events.
        event1 = NewEvent(type="OrderCreated", data=random_data())
        event2 = NewEvent(type="OrderUpdated", data=random_data())

        # Append two events to a different stream.
        stream_name = str(uuid4())
        self.client.append_events(stream_name, expected_position=None, events=[event1])
        self.client.append_events(stream_name, expected_position=0, events=[event2])

        # Read stream, expect two events.
        events = list(self.client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)

        # Expect stream position is an int.
        self.assertEqual(1, self.client.get_stream_position(stream_name))

        # Tombstone the stream, specifying "any" expected position.
        self.client.tombstone_stream(stream_name, expected_position=-1)

        # Can't call tombstone again.
        with self.assertRaises(GrpcError) as cm:
            self.client.tombstone_stream(stream_name, expected_position=-1)
        self.assertIn("is deleted", str(cm.exception))

        # Expect "stream not found" when reading tombstoned stream.
        # with self.assertRaises(StreamNotFound):
        #     list(self.client.read_stream_events(stream_name))
        # Actually get a GrpcError.
        with self.assertRaises(GrpcError) as cm:
            list(self.client.read_stream_events(stream_name))
        self.assertIn("is deleted", str(cm.exception))

        # Expect stream position is None.
        # self.assertEqual(None, self.client.get_stream_position(stream_name))
        # Actually get a GrpcError.
        with self.assertRaises(GrpcError) as cm:
            self.client.get_stream_position(stream_name)
        self.assertIn("is deleted", str(cm.exception))

    def test_tombstone_stream_expecting_stream_exists(self) -> None:
        self.construct_esdb_client()
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(self.client.read_stream_events(stream_name))

        # Can't tombstone stream that doesn't exist, while expecting "stream exists".
        with self.assertRaises(GrpcError) as cm:
            self.client.tombstone_stream(stream_name, expected_position=None)
        self.assertIn("Actual version: -1", str(cm.exception))

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
        with self.assertRaises(GrpcError) as cm:
            self.client.tombstone_stream(stream_name, expected_position=None)
        self.assertIn("is deleted", str(cm.exception))

        # Expect "stream not found" when reading tombstoned stream.
        # with self.assertRaises(StreamNotFound):
        #     list(self.client.read_stream_events(stream_name))
        # Actually get a GrpcError.
        with self.assertRaises(GrpcError) as cm:
            list(self.client.read_stream_events(stream_name))
        self.assertIn("is deleted", str(cm.exception))

        # Expect stream position is None.
        # self.assertEqual(None, self.client.get_stream_position(stream_name))
        # Actually get a GrpcError.
        with self.assertRaises(GrpcError) as cm:
            self.client.get_stream_position(stream_name)
        self.assertIn("is deleted", str(cm.exception))

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

        # For some reason "$tb" is now 2 rather than max_long.
        # Todo: Why is this?
        self.assertEqual(stream_metadata["$tb"], 2)

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

        with self.assertRaises(ExpectedPositionError):
            self.client.set_stream_metadata(
                stream_name=stream_name,
                metadata=stream_metadata,
                expected_position=10,
            )


class TestESDBClientInsecure(TestESDBClient):
    def construct_esdb_client(self) -> None:
        server_cert = None
        username = None
        password = None
        self.client = ESDBClient(
            host="localhost",
            port=2114,
            server_cert=server_cert,
            username=username,
            password=password,
        )


class TestSubscriptionReadRequest(TestCase):
    def test_ack_200_ids(self) -> None:
        read_request = SubscriptionReadRequest("group1")
        read_request_iter = read_request
        grpc_read_req = next(read_request_iter)
        self.assertIsInstance(grpc_read_req, grpc_persistent.ReadReq)

        # Do one batch of acks.
        event_ids: List[UUID] = []
        for _ in range(100):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.ack(event_id)
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 100)

        # Do another batch of acks.
        event_ids.clear()
        for _ in range(100):
            event_id = uuid4()
            event_ids.append(event_id)
            read_request.ack(event_id)
        grpc_read_req = next(read_request_iter)
        self.assertEqual(len(grpc_read_req.ack.ids), 100)


def random_data() -> bytes:
    return os.urandom(16)
