# -*- coding: utf-8 -*-
from unittest import TestCase
from uuid import UUID, uuid4

from esdbclient import NewEvent
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
