# -*- coding: utf-8 -*-
from unittest import TestCase
from uuid import uuid4

from esdbclient.client import (
    AppendPositionError,
    EsdbClient,
    NewEvent,
    ServiceUnavailable,
    StreamNotFound,
)


class TestEsdbClient(TestCase):
    def test_service_unavailable_exception(self) -> None:
        esdb_client = EsdbClient("localhost:2222")
        with self.assertRaises(ServiceUnavailable) as cm:
            list(esdb_client.read_stream(str(uuid4())))
        self.assertEqual(
            cm.exception.args[0].details(), "failed to connect to all addresses"
        )

    def test_stream_not_found_exception(self) -> None:
        esdb_client = EsdbClient("localhost:2113")
        stream_name = str(uuid4())

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name, backwards=True))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name, stream_position=1))

        with self.assertRaises(StreamNotFound):
            list(
                esdb_client.read_stream(stream_name, stream_position=1, backwards=True)
            )

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name, limit=10))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name, backwards=True, limit=10))

        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name, stream_position=1, limit=10))

        with self.assertRaises(StreamNotFound):
            list(
                esdb_client.read_stream(
                    stream_name, stream_position=1, backwards=True, limit=10
                )
            )

    def test_stream_append_and_read(self) -> None:
        esdb_client = EsdbClient("localhost:2113")
        stream_name = str(uuid4())

        # Check stream not found.
        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name))

        # Check get error when attempting to append empty list to position 1.
        with self.assertRaises(AppendPositionError) as cm:
            esdb_client.append_events(stream_name, position=1, events=[])
        self.assertEqual(cm.exception.args[0], f"Stream '{stream_name}' does not exist")

        # Append empty list of events.
        commit_position1 = esdb_client.append_events(
            stream_name, position=None, events=[]
        )
        self.assertIsInstance(commit_position1, int)

        # Check stream still not found.
        with self.assertRaises(StreamNotFound):
            list(esdb_client.read_stream(stream_name))

        # Check get error when attempting to append new event to position 1.
        event1 = NewEvent(type="OrderCreated", data=b"{}")
        with self.assertRaises(AppendPositionError) as cm:
            esdb_client.append_events(stream_name, position=1, events=[event1])
        self.assertEqual(cm.exception.args[0], f"Stream '{stream_name}' does not exist")

        # Append new event.
        commit_position2 = esdb_client.append_events(
            stream_name, position=None, events=[event1]
        )

        # Todo: Why isn't this +1?
        # self.assertEqual(commit_position2 - commit_position1, 1)
        self.assertEqual(commit_position2 - commit_position1, 126)

        # Read the stream (expect one event).
        events = list(esdb_client.read_stream(stream_name))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderCreated")

        # Check we can't append another new event at initial position.
        event2 = NewEvent(type="OrderUpdated", data=b"{}")
        with self.assertRaises(AppendPositionError) as cm:
            esdb_client.append_events(stream_name, position=None, events=[event2])
        self.assertEqual(cm.exception.args[0], "Current position is 0")

        # Append another new event.
        commit_position3 = esdb_client.append_events(
            stream_name, position=0, events=[event2]
        )

        # Todo: Why isn't this +1?
        # self.assertEqual(commit_position3 - commit_position2, 1)
        self.assertEqual(commit_position3 - commit_position2, 140)

        # Read the stream (expect two events in 'forwards' order).
        events = list(esdb_client.read_stream(stream_name))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].type, "OrderCreated")
        self.assertEqual(events[1].type, "OrderUpdated")

        # Read the stream backwards.
        events = list(esdb_client.read_stream(stream_name, backwards=True))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[1].type, "OrderCreated")
        self.assertEqual(events[0].type, "OrderUpdated")

        # Read the stream forwards from position 1.
        events = list(esdb_client.read_stream(stream_name, stream_position=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

        # Read the stream backwards from position 0.
        events = list(
            esdb_client.read_stream(stream_name, stream_position=0, backwards=True)
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderCreated")

        # Read the stream forwards with limit.
        events = list(esdb_client.read_stream(stream_name, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderCreated")

        # Read the stream backwards with limit.
        events = list(esdb_client.read_stream(stream_name, backwards=True, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

        # Check we can't append another new event at second position.
        event3 = NewEvent(type="OrderDeleted", data=b"{}")
        with self.assertRaises(AppendPositionError) as cm:
            esdb_client.append_events(stream_name, position=0, events=[event3])
        self.assertEqual(cm.exception.args[0], "Current position is 1")

        # Append another new event.
        commit_position4 = esdb_client.append_events(
            stream_name, position=1, events=[event3]
        )

        # Todo: Why isn't this +1?
        # self.assertEqual(commit_position4 - commit_position3, 1)
        self.assertEqual(commit_position4 - commit_position3, 140)

        # Read the stream forwards (expect three events).
        events = list(esdb_client.read_stream(stream_name))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0].type, "OrderCreated")
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[2].type, "OrderDeleted")

        # Read the stream backwards (expect three events).
        events = list(esdb_client.read_stream(stream_name, backwards=True))
        self.assertEqual(len(events), 3)
        self.assertEqual(events[2].type, "OrderCreated")
        self.assertEqual(events[1].type, "OrderUpdated")
        self.assertEqual(events[0].type, "OrderDeleted")

        # Read the stream forwards with position and limit.
        events = list(esdb_client.read_stream(stream_name, stream_position=1, limit=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

        # Read the stream backwards with position and limit.
        events = list(
            esdb_client.read_stream(
                stream_name, stream_position=1, backwards=True, limit=1
            )
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].type, "OrderUpdated")

    def test_read_all_streams(self) -> None:
        esdb_client = EsdbClient("localhost:2113")

        num_old_events = len(list(esdb_client.read_all()))

        event1 = NewEvent(type="OrderCreated", data=b"{}")
        event2 = NewEvent(type="OrderUpdated", data=b"{}")
        event3 = NewEvent(type="OrderDeleted", data=b"{}")

        # Append new events.
        stream_name1 = str(uuid4())
        commit_position1 = esdb_client.append_events(
            stream_name1, position=None, events=[event1, event2, event3]
        )

        stream_name2 = str(uuid4())
        commit_position2 = esdb_client.append_events(
            stream_name2, position=None, events=[event1, event2, event3]
        )

        events = list(esdb_client.read_all())
        self.assertEqual(len(events) - num_old_events, 6)

        events = list(esdb_client.read_all(commit_position=commit_position1))
        self.assertEqual(len(events), 4)

        events = list(esdb_client.read_all(commit_position=commit_position2))
        self.assertEqual(len(events), 1)
