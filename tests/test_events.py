from __future__ import annotations

import datetime
from unittest import TestCase
from uuid import UUID, uuid4

from google.protobuf import timestamp_pb2

from kurrentdbclient import CaughtUp, Checkpoint, FellBehind, NewEvent, RecordedEvent
from kurrentdbclient.protos.Grpc import streams_pb2 as grpc_streams
from kurrentdbclient.streams import BaseReadResponse
from tests.test_client import random_data


class TestNewEvent(TestCase):
    def test_construct_with_required_args_only(self) -> None:
        event_data = random_data()
        event_type = "OrderCreated"

        event = NewEvent(type=event_type, data=event_data)

        self.assertEqual(event.type, event_type)
        self.assertEqual(event.data, event_data)
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
            prepare_position=12346,
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
        self.assertEqual(recorded_event.prepare_position, 12346)
        self.assertEqual(recorded_event.retry_count, 5)
        self.assertEqual(recorded_event.link, None)

        self.assertEqual(recorded_event.ack_id, normal_event_id)
        self.assertFalse(recorded_event.is_system_event)
        self.assertFalse(recorded_event.is_link_event)
        self.assertFalse(recorded_event.is_resolved_event)
        self.assertFalse(recorded_event.is_checkpoint)
        self.assertFalse(recorded_event.is_caught_up)
        self.assertFalse(recorded_event.is_fell_behind)

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
            prepare_position=12346,
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
        self.assertEqual(recorded_event.prepare_position, 12346)
        self.assertEqual(recorded_event.retry_count, 5)
        self.assertEqual(recorded_event.link, None)

        self.assertEqual(recorded_event.ack_id, link_event_id)
        self.assertTrue(recorded_event.is_system_event)
        self.assertTrue(recorded_event.is_link_event)
        self.assertFalse(recorded_event.is_resolved_event)
        self.assertFalse(recorded_event.is_checkpoint)
        self.assertFalse(recorded_event.is_caught_up)
        self.assertFalse(recorded_event.is_fell_behind)

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
            prepare_position=12346,
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
            prepare_position=12346,
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
        self.assertFalse(recorded_event.is_fell_behind)

        link = recorded_event.link
        assert link is not None  # For mypy.
        self.assertEqual(link.id, link_event_id)
        self.assertTrue(link.is_system_event)
        self.assertTrue(link.is_link_event)
        self.assertFalse(link.is_resolved_event)
        self.assertFalse(link.is_checkpoint)
        self.assertFalse(link.is_caught_up)
        self.assertFalse(link.is_fell_behind)


class TestCheckpoint(TestCase):
    def test(self) -> None:
        checkpoint = Checkpoint(
            commit_position=12345, prepare_position=12346, recorded_at=None
        )
        self.assertEqual(checkpoint.commit_position, 12345)
        self.assertEqual(checkpoint.prepare_position, 12346)
        self.assertTrue(checkpoint.is_checkpoint)
        self.assertFalse(checkpoint.is_caught_up)
        self.assertFalse(checkpoint.is_fell_behind)
        self.assertIsNone(checkpoint.recorded_at)

        now = datetime.datetime.now()
        checkpoint = Checkpoint(
            commit_position=67890, prepare_position=67891, recorded_at=now
        )
        self.assertEqual(checkpoint.commit_position, 67890)
        self.assertEqual(checkpoint.prepare_position, 67891)
        self.assertEqual(now, checkpoint.recorded_at)


class TestCaughtUp(TestCase):
    def test(self) -> None:
        caught_up = CaughtUp(
            stream_position=10,
            commit_position=12345,
            prepare_position=12346,
            recorded_at=None,
        )
        self.assertFalse(caught_up.is_checkpoint)
        self.assertTrue(caught_up.is_caught_up)
        self.assertEqual(caught_up.stream_position, 10)
        self.assertEqual(caught_up.commit_position, 12345)
        self.assertEqual(caught_up.prepare_position, 12346)
        self.assertIsNone(caught_up.recorded_at)

        now = datetime.datetime.now()
        caught_up = CaughtUp(
            stream_position=10,
            commit_position=12345,
            prepare_position=12346,
            recorded_at=now,
        )
        self.assertFalse(caught_up.is_checkpoint)
        self.assertTrue(caught_up.is_caught_up)
        self.assertEqual(caught_up.stream_position, 10)
        self.assertEqual(caught_up.commit_position, 12345)
        self.assertEqual(caught_up.prepare_position, 12346)
        self.assertEqual(now, caught_up.recorded_at)


class TestFellBehind(TestCase):
    def test(self) -> None:
        caught_up = FellBehind(
            stream_position=10,
            commit_position=12345,
            prepare_position=12346,
            recorded_at=None,
        )
        self.assertFalse(caught_up.is_checkpoint)
        self.assertTrue(caught_up.is_fell_behind)
        self.assertEqual(caught_up.stream_position, 10)
        self.assertEqual(caught_up.commit_position, 12345)
        self.assertEqual(caught_up.prepare_position, 12346)
        self.assertIsNone(caught_up.recorded_at)

        now = datetime.datetime.now()
        caught_up = FellBehind(
            stream_position=10,
            commit_position=12345,
            prepare_position=12346,
            recorded_at=now,
        )
        self.assertFalse(caught_up.is_checkpoint)
        self.assertTrue(caught_up.is_fell_behind)
        self.assertEqual(caught_up.stream_position, 10)
        self.assertEqual(caught_up.commit_position, 12345)
        self.assertEqual(caught_up.prepare_position, 12346)
        self.assertEqual(now, caught_up.recorded_at)


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
            prepare_position=12346,
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
            prepare_position=12346,
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


class TestBaseReadResponse(TestCase):
    class ReadResponse(BaseReadResponse):
        def __init__(
            self,
            *,
            include_checkpoints: bool = False,
            include_caught_up: bool = False,
            include_fell_behind: bool = False,
        ) -> None:
            super().__init__(stream_name="stream_name")
            self._include_checkpoints = include_checkpoints
            self._include_caught_up = include_caught_up
            self._include_fell_behind = include_fell_behind

        def convert_read_resp(
            self, read_resp: grpc_streams.ReadResp
        ) -> RecordedEvent | None:
            return super()._convert_read_resp(read_resp)

        def filter_recorded_event(
            self, recorded_event: RecordedEvent
        ) -> RecordedEvent | None:
            return super()._filter_recorded_event(recorded_event)

    def test_read_response_checkpoint(self) -> None:
        read_response = self.ReadResponse(include_checkpoints=False)
        timestamp = timestamp_pb2.Timestamp()
        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp.FromDatetime(dt=now)
        read_resp = grpc_streams.ReadResp(
            checkpoint=grpc_streams.ReadResp.Checkpoint(
                timestamp=timestamp,
                commit_position=11,
                prepare_position=12,
            )
        )
        recorded_event = read_response.convert_read_resp(read_resp)
        self.assertIsInstance(recorded_event, Checkpoint)
        assert isinstance(recorded_event, Checkpoint)
        self.assertEqual(now, recorded_event.recorded_at)
        self.assertEqual(11, recorded_event.commit_position)
        self.assertEqual(12, recorded_event.prepare_position)
        self.assertIsNone(read_response.filter_recorded_event(recorded_event))
        read_response = self.ReadResponse(include_checkpoints=True)
        self.assertEqual(
            recorded_event, read_response.filter_recorded_event(recorded_event)
        )

        # Check null timestamp is converted to None.
        timestamp = timestamp_pb2.Timestamp()
        read_resp = grpc_streams.ReadResp(
            checkpoint=grpc_streams.ReadResp.Checkpoint(
                timestamp=timestamp,
                commit_position=11,
                prepare_position=12,
            )
        )

        read_response = self.ReadResponse()
        recorded_event = read_response.convert_read_resp(read_resp)
        assert isinstance(recorded_event, Checkpoint)
        self.assertIsNone(recorded_event.recorded_at)

    def test_read_response_caught_up(self) -> None:
        read_response = self.ReadResponse(include_caught_up=False)
        timestamp = timestamp_pb2.Timestamp()
        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp.FromDatetime(dt=now)
        read_resp = grpc_streams.ReadResp(
            caught_up=grpc_streams.ReadResp.CaughtUp(
                timestamp=timestamp,
                stream_revision=10,
                position=grpc_streams.ReadResp.Position(
                    commit_position=11,
                    prepare_position=12,
                ),
            )
        )
        recorded_event = read_response.convert_read_resp(read_resp)
        self.assertIsInstance(recorded_event, CaughtUp)
        assert isinstance(recorded_event, CaughtUp)
        self.assertEqual(now, recorded_event.recorded_at)
        self.assertEqual(10, recorded_event.stream_position)
        self.assertEqual(11, recorded_event.commit_position)
        self.assertEqual(12, recorded_event.prepare_position)
        self.assertIsNone(read_response.filter_recorded_event(recorded_event))
        read_response = self.ReadResponse(include_caught_up=True)
        self.assertEqual(
            recorded_event, read_response.filter_recorded_event(recorded_event)
        )

        # Check null timestamp is converted to None.
        timestamp = timestamp_pb2.Timestamp()
        read_resp = grpc_streams.ReadResp(
            caught_up=grpc_streams.ReadResp.CaughtUp(
                timestamp=timestamp,
                stream_revision=10,
                position=grpc_streams.ReadResp.Position(
                    commit_position=11,
                    prepare_position=12,
                ),
            )
        )

        read_response = self.ReadResponse()
        recorded_event = read_response.convert_read_resp(read_resp)
        assert isinstance(recorded_event, CaughtUp)
        self.assertIsNone(recorded_event.recorded_at)

    def test_read_response_fell_behind(self) -> None:
        read_response = self.ReadResponse(include_fell_behind=False)
        timestamp = timestamp_pb2.Timestamp()
        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp.FromDatetime(dt=now)
        read_resp = grpc_streams.ReadResp(
            fell_behind=grpc_streams.ReadResp.FellBehind(
                timestamp=timestamp,
                stream_revision=10,
                position=grpc_streams.ReadResp.Position(
                    commit_position=11,
                    prepare_position=12,
                ),
            )
        )
        recorded_event = read_response.convert_read_resp(read_resp)
        self.assertIsInstance(recorded_event, FellBehind)
        assert isinstance(recorded_event, FellBehind)
        self.assertEqual(now, recorded_event.recorded_at)
        self.assertEqual(10, recorded_event.stream_position)
        self.assertEqual(11, recorded_event.commit_position)
        self.assertEqual(12, recorded_event.prepare_position)
        self.assertIsNone(read_response.filter_recorded_event(recorded_event))
        read_response = self.ReadResponse(include_fell_behind=True)
        self.assertEqual(
            recorded_event, read_response.filter_recorded_event(recorded_event)
        )

        # Check null timestamp is converted to None.
        timestamp = timestamp_pb2.Timestamp()
        read_resp = grpc_streams.ReadResp(
            fell_behind=grpc_streams.ReadResp.FellBehind(
                timestamp=timestamp,
                stream_revision=10,
                position=grpc_streams.ReadResp.Position(
                    commit_position=11,
                    prepare_position=12,
                ),
            )
        )

        read_response = self.ReadResponse()
        recorded_event = read_response.convert_read_resp(read_resp)
        assert isinstance(recorded_event, FellBehind)
        self.assertIsNone(recorded_event.recorded_at)

    def test_read_response_other(self) -> None:
        read_resp = grpc_streams.ReadResp(last_stream_position=10)
        read_response = self.ReadResponse()
        self.assertIsNone(read_response.convert_read_resp(read_resp))
