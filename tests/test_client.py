# -*- coding: utf-8 -*-
from typing import Any
from unittest import TestCase
from uuid import uuid4

from grpc import Call, RpcError, StatusCode

from esdbclient.client import (
    DeadlineExceeded,
    EsdbClient,
    ExpectedPositionError,
    GrpcError,
    NewEvent,
    ServiceUnavailable,
    StreamNotFound,
    handle_rpc_error,
)


class TestEsdbClient(TestCase):
    def test_service_unavailable_exception(self) -> None:
        esdb_client = EsdbClient("localhost:2222")

        with self.assertRaises(ServiceUnavailable) as cm:
            list(esdb_client.read_stream_events(str(uuid4())))
        self.assertEqual(
            cm.exception.args[0].details(), "failed to connect to all addresses"
        )

        with self.assertRaises(ServiceUnavailable) as cm:
            esdb_client.append_events(str(uuid4()), expected_position=None, events=[])
        self.assertEqual(
            cm.exception.args[0].details(), "failed to connect to all addresses"
        )

    def test_handle_deadline_exceeded_error(self) -> None:
        class DeadlineExceededRpcError(RpcError, Call):
            def initial_metadata(self) -> None:
                pass

            def trailing_metadata(self) -> None:
                pass

            def code(self) -> StatusCode:
                return StatusCode.DEADLINE_EXCEEDED

            def details(self) -> None:
                pass

            def is_active(self) -> None:
                pass

            def time_remaining(self) -> None:
                pass

            def cancel(self) -> None:
                pass

            def add_callback(self, callback: Any) -> None:
                pass

        with self.assertRaises(GrpcError) as cm:
            handle_rpc_error(DeadlineExceededRpcError())
        self.assertEqual(cm.exception.__class__, DeadlineExceeded)

    def test_handle_unavailable_error(self) -> None:
        class UnavailableRpcError(RpcError, Call):
            def initial_metadata(self) -> None:
                pass

            def trailing_metadata(self) -> None:
                pass

            def code(self) -> StatusCode:
                return StatusCode.UNAVAILABLE

            def details(self) -> None:
                pass

            def is_active(self) -> None:
                pass

            def time_remaining(self) -> None:
                pass

            def cancel(self) -> None:
                pass

            def add_callback(self, callback: Any) -> None:
                pass

        with self.assertRaises(GrpcError) as cm:
            handle_rpc_error(UnavailableRpcError())
        self.assertEqual(cm.exception.__class__, ServiceUnavailable)

    def test_handle_other_call_error(self) -> None:
        class OtherRpcError(RpcError, Call):
            def initial_metadata(self) -> None:
                pass

            def trailing_metadata(self) -> None:
                pass

            def code(self) -> int:
                return -1

            def details(self) -> None:
                pass

            def is_active(self) -> None:
                pass

            def time_remaining(self) -> None:
                pass

            def cancel(self) -> None:
                pass

            def add_callback(self, callback: Any) -> None:
                pass

        with self.assertRaises(GrpcError) as cm:
            handle_rpc_error(OtherRpcError())
        self.assertEqual(cm.exception.__class__, GrpcError)

    def test_handle_non_call_rpc_error(self) -> None:

        # Check non-Call errors are handled.
        class MyRpcError(RpcError):
            pass

        msg = "some non-Call error"
        with self.assertRaises(GrpcError) as cm:
            handle_rpc_error(MyRpcError(msg))
        self.assertEqual(cm.exception.__class__, GrpcError)
        self.assertIsInstance(cm.exception.args[0], MyRpcError)

    def test_stream_not_found_exception(self) -> None:
        esdb_client = EsdbClient("localhost:2113")
        stream_name = str(uuid4())

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream_events(stream_name))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream_events(stream_name, backwards=True))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream_events(stream_name, position=1))

        with self.assertRaises(StreamNotFound):
            list(
                esdb_client.read_stream_events(stream_name, position=1, backwards=True)
            )

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream_events(stream_name, limit=10))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream_events(stream_name, backwards=True, limit=10))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream_events(stream_name, position=1, limit=10))

        with self.assertRaises(StreamNotFound):
            list(
                esdb_client.read_stream_events(
                    stream_name, position=1, backwards=True, limit=10
                )
            )

    def test_stream_append_and_read_without_occ(self) -> None:
        client = EsdbClient("localhost:2113")
        stream_name = str(uuid4())

        event1 = NewEvent(type="Snapshot", data=b"{}", metadata=b"{}")

        # Append new event.
        client.append_events(stream_name, expected_position=-1, events=[event1])
        events = list(client.read_stream_events(stream_name, backwards=True, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "Snapshot")

    def test_stream_append_and_read_with_occ(self) -> None:
        client = EsdbClient("localhost:2113")
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(client.read_stream_events(stream_name))

        # Check stream position is None.
        self.assertEqual(client.get_stream_position(stream_name), None)

        # Check get error when attempting to append empty list to position 1.
        with self.assertRaises(ExpectedPositionError) as cm:
            client.append_events(stream_name, expected_position=1, events=[])
        self.assertEqual(cm.exception.args[0], f"Stream '{stream_name}' does not exist")

        # Append empty list of events.
        commit_position1 = client.append_events(
            stream_name, expected_position=None, events=[]
        )
        self.assertIsInstance(commit_position1, int)

        # Check stream still not found.
        with self.assertRaises(StreamNotFound):
            list(client.read_stream_events(stream_name))

        # Check stream position is None.
        self.assertEqual(client.get_stream_position(stream_name), None)

        # Check get error when attempting to append new event to position 1.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        with self.assertRaises(ExpectedPositionError) as cm:
            client.append_events(stream_name, expected_position=1, events=[event1])
        self.assertEqual(cm.exception.args[0], f"Stream '{stream_name}' does not exist")

        # Append new event.
        commit_position2 = client.append_events(
            stream_name, expected_position=None, events=[event1]
        )

        # Todo: Why isn't this +1?
        # self.assertEqual(commit_position2 - commit_position1, 1)
        self.assertEqual(commit_position2 - commit_position1, 126)

        # Check stream position is 0.
        self.assertEqual(client.get_stream_position(stream_name), 0)

        # Read the stream forwards from the start (expect one event).
        events = list(client.read_stream_events(stream_name))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderCreated")

        # Check we can't append another new event at initial position.
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        with self.assertRaises(ExpectedPositionError) as cm:
            client.append_events(stream_name, expected_position=None, events=[event2])
        self.assertEqual(cm.exception.args[0], "Current position is 0")

        # Append another event.
        commit_position3 = client.append_events(
            stream_name, expected_position=0, events=[event2]
        )

        # Check stream position is 1.
        self.assertEqual(client.get_stream_position(stream_name), 1)

        # NB: Why isn't this +1? because it's "disk position" :-|
        # self.assertEqual(commit_position3 - commit_position2, 1)
        self.assertEqual(commit_position3 - commit_position2, 142)

        # Read the stream (expect two events in 'forwards' order).
        events = list(client.read_stream_events(stream_name))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].type, "OrderCreated")
        self.assertEqual(events[1].type, "OrderUpdated")

        # Read the stream backwards from the end.
        events = list(client.read_stream_events(stream_name, backwards=True))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[0].type, "OrderUpdated")

        # Read the stream forwards from position 1.
        events = list(client.read_stream_events(stream_name, position=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

        # Read the stream backwards from position 0.
        events = list(
            client.read_stream_events(stream_name, position=0, backwards=True)
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderCreated")

        # Read the stream forwards from start with limit.
        events = list(client.read_stream_events(stream_name, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderCreated")

        # Read the stream backwards from end with limit.
        events = list(client.read_stream_events(stream_name, backwards=True, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

        # Check we can't append another new event at second position.
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        with self.assertRaises(ExpectedPositionError) as cm:
            client.append_events(stream_name, expected_position=0, events=[event3])
        self.assertEqual(cm.exception.args[0], "Current position is 1")

        # Append another new event.
        commit_position4 = client.append_events(
            stream_name, expected_position=1, events=[event3]
        )

        # Check stream position is 2.
        self.assertEqual(client.get_stream_position(stream_name), 2)

        # NB: Why isn't this +1? because it's "disk position" :-|
        # self.assertEqual(commit_position4 - commit_position3, 1)
        self.assertEqual(commit_position4 - commit_position3, 142)

        # Read the stream forwards from start (expect three events).
        events = list(client.read_stream_events(stream_name))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].type, "OrderCreated")
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[2].type, "OrderDeleted")

        # Read the stream backwards from end (expect three events).
        events = list(client.read_stream_events(stream_name, backwards=True))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[2].type, "OrderCreated")
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[0].type, "OrderDeleted")

        # Read the stream forwards from position with limit.
        events = list(client.read_stream_events(stream_name, position=1, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

        # Read the stream backwards from position withm limit.
        events = list(
            client.read_stream_events(stream_name, position=1, backwards=True, limit=1)
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

    def test_read_all_events(self) -> None:
        esdb_client = EsdbClient("localhost:2113")

        num_old_events = len(list(esdb_client.read_all_events()))

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position1 = esdb_client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        commit_position2 = esdb_client.append_events(
            stream_name2, expected_position=None, events=[event1, event2, event3]
        )

        # Check we can read forwards from the start.
        events = list(esdb_client.read_all_events())
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
        events = list(esdb_client.read_all_events(backwards=True))
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
        events = list(esdb_client.read_all_events(position=commit_position1))
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
        events = list(esdb_client.read_all_events(position=commit_position2))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")

        # Check we can read backwards from commit position 1.
        # NB backwards here doesn't include event at commit position.
        events = list(
            esdb_client.read_all_events(position=commit_position1, backwards=True)
        )
        self.assertEqual(len(events) - num_old_events, 2)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name1)
        self.assertEqual(events[1].type, "OrderCreated")

        # Check we can read backwards from commit position 2.
        # NB backwards here doesn't include event at commit position.
        events = list(
            esdb_client.read_all_events(position=commit_position2, backwards=True)
        )
        self.assertEqual(len(events) - num_old_events, 5)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name1)
        self.assertEqual(events[2].type, "OrderDeleted")

        # Check we can read forwards from the start with limit.
        events = list(esdb_client.read_all_events(limit=3))
        self.assertEqual(len(events), 3)

        # Check we can read backwards from the end with limit.
        events = list(esdb_client.read_all_events(backwards=True, limit=3))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderCreated")

        # Check we can read forwards from commit position 1 with limit.
        events = list(esdb_client.read_all_events(position=commit_position1, limit=3))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name1)
        self.assertEqual(events[0].type, "OrderDeleted")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name2)
        self.assertEqual(events[2].type, "OrderUpdated")

        # Check we can read backwards from commit position 2 with limit.
        events = list(
            esdb_client.read_all_events(
                position=commit_position2, backwards=True, limit=3
            )
        )
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].stream_name, stream_name2)
        self.assertEqual(events[0].type, "OrderUpdated")
        self.assertEqual(events[1].stream_name, stream_name2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[2].stream_name, stream_name1)
        self.assertEqual(events[2].type, "OrderDeleted")

    def test_read_all_filter_include(self) -> None:
        client = EsdbClient("localhost:2113")

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Read only OrderCreated.
        events = list(client.read_all_events(filter_include=("OrderCreated",)))
        types = set([e.type for e in events])
        self.assertEqual(types, {"OrderCreated"})

        # Read only OrderCreated and OrderDeleted.
        events = list(
            client.read_all_events(filter_include=("OrderCreated", "OrderDeleted"))
        )
        types = set([e.type for e in events])
        self.assertEqual(types, {"OrderCreated", "OrderDeleted"})

    def test_read_all_filter_exclude(self) -> None:
        client = EsdbClient("localhost:2113")

        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Exclude OrderCreated.
        events = list(client.read_all_events(filter_exclude=("OrderCreated",)))
        types = set([e.type for e in events])
        self.assertNotIn("OrderCreated", types)
        self.assertIn("OrderUpdated", types)
        self.assertIn("OrderDeleted", types)

        # Exclude OrderCreated and OrderDeleted.
        events = list(
            client.read_all_events(filter_exclude=("OrderCreated", "OrderDeleted"))
        )
        types = set([e.type for e in events])
        self.assertNotIn("OrderCreated", types)
        self.assertIn("OrderUpdated", types)
        self.assertNotIn("OrderDeleted", types)

    def test_subscribe_all_events_default_filter(self) -> None:
        client = EsdbClient("localhost:2113")

        event1 = NewEvent(type="OrderCreated", data=b"{a}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{b}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{c}", metadata=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe to all events, from the start.
        subscription = client.subscribe_all_events()

        # Iterate over the first three events.
        events = []
        for event in subscription:
            events.append(event)
            if len(events) == 3:
                break

        # Get the current commit position.
        commit_position = client.get_commit_position()

        # Subscribe from the current commit position.
        subscription = client.subscribe_all_events(position=commit_position)

        # Append three more events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name2 = str(uuid4())
        client.append_events(
            stream_name2, expected_position=None, events=[event1, event2, event3]
        )

        # Check the stream name of the newly received events.
        events = []
        for event in subscription:
            self.assertEqual(event.stream_name, stream_name2)
            events.append(event)
            self.assertIn(event.type, ["OrderCreated", "OrderUpdated", "OrderDeleted"])
            if len(events) == 3:
                break

    def test_subscribe_all_events_no_filter(self) -> None:
        client = EsdbClient("localhost:2113")

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the current commit position.
        subscription = client.subscribe_all_events(
            filter_exclude=[],
            filter_include=[],
        )

        # Expect to get system events.
        for event in subscription:
            if event.type.startswith("$"):
                break

    def test_subscribe_all_events_include_filter(self) -> None:
        client = EsdbClient("localhost:2113")

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the beginning.
        subscription = client.subscribe_all_events(
            filter_exclude=[],
            filter_include=["OrderCreated"],
        )

        # Expect to only get "OrderCreated" events.
        count = 0
        for event in subscription:
            if not event.type.startswith("OrderCreated"):
                self.fail(f"Include filter is broken: {event.type}")
            count += 1
            break
        self.assertEqual(count, 1)

    def test_subscribe_all_events_from_commit_position_zero(self) -> None:
        client = EsdbClient("localhost:2113")

        # Append new events.
        event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")
        stream_name1 = str(uuid4())
        client.append_events(
            stream_name1, expected_position=None, events=[event1, event2, event3]
        )

        # Subscribe from the beginning.
        subscription = client.subscribe_all_events(
            position=0,
        )

        # Expect to only get "OrderCreated" events.
        count = 0
        for _ in subscription:
            count += 1
            break
        self.assertEqual(count, 1)
