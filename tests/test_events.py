# -*- coding: utf-8 -*-
from unittest import TestCase
from uuid import UUID, uuid4

from esdbclient import Checkpoint, NewEvent, RecordedEvent
from esdbclient.events import CaughtUp
from tests.test_client import random_data


class TestNewEvent(TestCase):
    def test_construct_with_required_args_only(self) -> None:
        data = random_data()
        type = "OrderCreated"

        event = NewEvent(type=type, data=data)

        self.assertEqual(event.type, type)
        self.assertEqual(event.data, event.data)
        self.assertEqual(event.metadata, b"")
        self.assertEqual(event.content_type, "application/json")
        self.assertIsInstance(event.id, UUID)

    def test_construct_with_optional_metadata(self) -> None:
        metadata = random_data()

        event = NewEvent(type="OrderUpdated", data=random_data(), metadata=metadata)

        self.assertEqual(event.metadata, metadata)

    def test_construct_with_optional_id(self) -> None:
        event_id = uuid4()

        event = NewEvent(
            type="OrderDeleted",
            data=random_data(),
            metadata=random_data(),
            id=event_id,
        )

        self.assertEqual(event.id, event_id)

    def test_construct_with_optional_content_type(self) -> None:
        event = NewEvent(
            type="OrderDeleted",
            data=random_data(),
            content_type="application/octet-stream",
        )
        self.assertEqual(event.content_type, "application/octet-stream")


class TestRecordedEvent(TestCase):
    def test_normal_event(self) -> None:
        normal_event_id = uuid4()
        recorded_event = RecordedEvent(
            type="type1",
            data=b'{"a": "b"}',
            metadata=b'{"c": "d"}',
            content_type="application/json",
            id=normal_event_id,
            stream_name="stream1",
            stream_position=12,
            commit_position=12345,
            retry_count=5,
            link=None,
        )
        self.assertEqual(recorded_event.type, "type1")
        self.assertEqual(recorded_event.data, b'{"a": "b"}')
        self.assertEqual(recorded_event.metadata, b'{"c": "d"}')
        self.assertEqual(recorded_event.content_type, "application/json")
        self.assertEqual(recorded_event.id, normal_event_id)
        self.assertEqual(recorded_event.stream_name, "stream1")
        self.assertEqual(recorded_event.stream_position, 12)
        self.assertEqual(recorded_event.commit_position, 12345)
        self.assertEqual(recorded_event.retry_count, 5)
        self.assertEqual(recorded_event.link, None)

        self.assertEqual(recorded_event.ack_id, normal_event_id)
        self.assertFalse(recorded_event.is_system_event)
        self.assertFalse(recorded_event.is_link_event)
        self.assertFalse(recorded_event.is_resolved_event)
        self.assertFalse(recorded_event.is_checkpoint)
        self.assertFalse(recorded_event.is_caught_up)

    def test_link_event(self) -> None:
        link_event_id = uuid4()
        recorded_event = RecordedEvent(
            type="$>",
            data=b'{"a": "b"}',
            metadata=b'{"c": "d"}',
            content_type="application/json",
            id=link_event_id,
            stream_name="stream1",
            stream_position=12,
            commit_position=12345,
            retry_count=5,
            link=None,
        )
        self.assertEqual(recorded_event.type, "$>")
        self.assertEqual(recorded_event.data, b'{"a": "b"}')
        self.assertEqual(recorded_event.metadata, b'{"c": "d"}')
        self.assertEqual(recorded_event.content_type, "application/json")
        self.assertEqual(recorded_event.id, link_event_id)
        self.assertEqual(recorded_event.stream_name, "stream1")
        self.assertEqual(recorded_event.stream_position, 12)
        self.assertEqual(recorded_event.commit_position, 12345)
        self.assertEqual(recorded_event.retry_count, 5)
        self.assertEqual(recorded_event.link, None)

        self.assertEqual(recorded_event.ack_id, link_event_id)
        self.assertTrue(recorded_event.is_system_event)
        self.assertTrue(recorded_event.is_link_event)
        self.assertFalse(recorded_event.is_resolved_event)
        self.assertFalse(recorded_event.is_checkpoint)
        self.assertFalse(recorded_event.is_caught_up)

    def test_resolved_event(self) -> None:
        normal_event_id = uuid4()
        link_event_id = uuid4()
        recorded_event = RecordedEvent(
            type="$>",
            data=b'{"a": "b"}',
            metadata=b'{"c": "d"}',
            content_type="application/json",
            id=link_event_id,
            stream_name="stream1",
            stream_position=12,
            commit_position=12345,
            retry_count=5,
            link=None,
        )
        recorded_event = RecordedEvent(
            type="type1",
            data=b'{"a": "b"}',
            metadata=b'{"c": "d"}',
            content_type="application/json",
            id=normal_event_id,
            stream_name="stream1",
            stream_position=12,
            commit_position=12345,
            retry_count=5,
            link=recorded_event,
        )
        self.assertEqual(recorded_event.type, "type1")
        self.assertEqual(recorded_event.id, normal_event_id)
        self.assertEqual(recorded_event.ack_id, link_event_id)

        self.assertFalse(recorded_event.is_system_event)
        self.assertFalse(recorded_event.is_link_event)
        self.assertTrue(recorded_event.is_resolved_event)
        self.assertFalse(recorded_event.is_checkpoint)
        self.assertFalse(recorded_event.is_caught_up)

        link = recorded_event.link
        assert link is not None  # For mypy.
        self.assertEqual(link.id, link_event_id)
        self.assertTrue(link.is_system_event)
        self.assertTrue(link.is_link_event)
        self.assertFalse(link.is_resolved_event)
        self.assertFalse(link.is_checkpoint)
        self.assertFalse(link.is_caught_up)


class TestCheckpoint(TestCase):
    def test(self) -> None:
        checkpoint = Checkpoint(commit_position=12345)
        self.assertEqual(checkpoint.commit_position, 12345)
        self.assertTrue(checkpoint.is_checkpoint)
        self.assertFalse(checkpoint.is_caught_up)

        checkpoint = Checkpoint(commit_position=67890)
        self.assertEqual(checkpoint.commit_position, 67890)


class TestCaughtUp(TestCase):
    def test(self) -> None:
        caught_up = CaughtUp()
        self.assertFalse(caught_up.is_checkpoint)
        self.assertTrue(caught_up.is_caught_up)


class TestEquality(TestCase):
    def test(self) -> None:
        new_event1 = NewEvent(type="Type1", data=b"{}")
        new_event2 = NewEvent(type="Type2", data=b"{}")
        recorded_event1 = RecordedEvent(
            type="type1",
            data=b'{"a": "b"}',
            metadata=b'{"c": "d"}',
            content_type="application/json",
            id=new_event1.id,
            stream_name="stream1",
            stream_position=12,
            commit_position=12345,
            retry_count=5,
            link=None,
        )
        recorded_event2 = RecordedEvent(
            type="type1",
            data=b'{"a": "b"}',
            metadata=b'{"c": "d"}',
            content_type="application/json",
            id=new_event2.id,
            stream_name="stream1",
            stream_position=12,
            commit_position=12345,
            retry_count=5,
            link=None,
        )

        self.assertEqual(new_event1, recorded_event1)
        self.assertEqual(recorded_event1, new_event1)
        self.assertEqual(new_event2, recorded_event2)
        self.assertEqual(recorded_event2, new_event2)
        self.assertNotEqual(new_event2, recorded_event1)
        self.assertNotEqual(recorded_event1, new_event2)
        self.assertNotEqual(new_event1, recorded_event2)
        self.assertNotEqual(recorded_event2, new_event1)
