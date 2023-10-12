# -*- coding: utf-8 -*-
import os
from typing import List
from unittest import TestCase
from uuid import UUID, uuid4

from esdbclient import EventStoreDBClient, NewEvent, StreamState
from tests.test_client import get_ca_certificate, get_server_certificate


def random_data() -> bytes:
    return os.urandom(16)


class TestPersistentSubscriptionACK(TestCase):
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

    def setUp(self) -> None:
        self.construct_esdb_client()

    def tearDown(self) -> None:
        super().tearDown()
        if hasattr(self, "client"):
            self.client.close()

    def given_subscription(self) -> str:
        group_name = f"my-subscription-{uuid4().hex}"
        # print(f"  Given subscription {group_name}")
        self.client.create_subscription_to_all(group_name=group_name, from_end=True)
        return group_name

    def when_append_new_events(self, *data: bytes) -> List[UUID]:
        events = [NewEvent(type="AnEvent", data=d, metadata=b"{}") for d in data]
        self.client.append_events(
            str(uuid4()),
            current_version=StreamState.NO_STREAM,
            events=events,
        )
        ids = [e.id for e in events]
        # print(f"  When appending events:\n    {', '.join(str(id) for id in ids)}")
        return ids

    def then_consumer_receives_and_acks(
        self,
        expected_ids: List[UUID],
        on: str,
    ) -> None:
        # print(
        #     f"  Then consumer of {on} should receive:\n   "
        #     f" {', '.join(str(id) for id in expected_ids)}"
        # )
        unexpected_ids = []
        with self.client.read_subscription_to_all(group_name=on) as subscription:
            expected_received_count = 0
            for _, event in enumerate(subscription, start=1):
                # print(f"    > Received: {event.id!s}")
                if event.id not in expected_ids:
                    unexpected_ids.append(event.id)
                else:
                    subscription.ack(event.id)
                    expected_received_count += 1
                if expected_received_count == len(expected_ids):
                    break
        if unexpected_ids:
            msg = "Unexpected events were received:\n"
            for unexpected_id in unexpected_ids:
                msg += f"{unexpected_id!r} not expected {expected_ids!s}\n"
            self.fail(msg)

    def test_event_is_removed_from_subscription_after_ack(self) -> None:
        # print("Scenario: Persistent sub. consumer doesn't receive ACKed events")
        subscription = self.given_subscription()

        first_batch = self.when_append_new_events(random_data(), random_data())
        self.then_consumer_receives_and_acks(first_batch, on=subscription)

        second_batch = self.when_append_new_events(random_data(), random_data())
        self.then_consumer_receives_and_acks(second_batch, on=subscription)
