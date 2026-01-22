import unittest
import uuid
from datetime import datetime
from unittest.async_case import IsolatedAsyncioTestCase

from kurrentdbclient import AsyncKurrentDBClient, NewEvent, StreamState
from tests.test_client import (
    KurrentDBClientTestCase,
    get_server_certificate,
    random_data,
)

# NUM_BYTES = [3, 30, 300, 3000]
NUM_BYTES = [25]
# NUM_EVENTS = [1, 100, 1000, 10000]
NUM_EVENTS = [10000]


class Benchmark(KurrentDBClientTestCase):
    # KDB_TARGET = "localhost:2113"
    # KDB_TLS = False

    def test_benchmark(self) -> None:
        print("Benchmarking client with blocking I/0...")
        print()

        self.construct_client()
        # stream_name = "benchmark-" + uuid.uuid4().hex
        stream_name = "benchmark-static-000001"
        print("Warming up...")
        print()
        # self.write_events(stream_name, 1, 3)
        # self.read_events(stream_name, 1)
        print()
        for num_bytes in NUM_BYTES:
            print()
            print("Events with", num_bytes, "bytes of data...")
            print()
            for _ in range(2000):
                for num_events in NUM_EVENTS:
                    # stream_name = "benchmark-" + uuid.uuid4().hex
                    stream_name = "benchmark-static-000002"
                    # self.write_events(stream_name, num_events, num_bytes)
                    self.read_events(stream_name, num_events)
                    print()

    def write_events(self, stream_name: str, num_events: int, num_bytes: int) -> None:
        # print("Writing", num_events, "events with", num_bytes, "bytes of data...")
        start = datetime.now()
        for i in range(num_events):
            new_event = NewEvent(type="BenchmarkEvent", data=random_data(num_bytes))
            self.client.append_event(
                stream_name,
                current_version=StreamState.NO_STREAM if not i else i - 1,
                event=new_event,
            )
        end = datetime.now()
        duration = (end - start).total_seconds()
        print(
            "Wrote",
            num_events,
            "events in:",
            duration,
            "seconds,",
            num_events / duration,
            "events/s",
        )

    def read_events(self, stream_name: str, num_events: int) -> None:
        # print("Reading", num_events, "events...")
        start = datetime.now()
        i = 0
        for _ in self.client.read_stream(stream_name):
            i += 1
        assert i == num_events
        end = datetime.now()
        duration = (end - start).total_seconds()
        print(
            "Read",
            num_events,
            "events in: ",
            duration,
            "seconds,",
            num_events / duration,
            "events/s",
        )


class AsyncBenchmark(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = AsyncKurrentDBClient(
            uri="kdb://admin:changeit@localhost:2114",
            root_certificates=get_server_certificate("localhost:2114"),
        )

    async def _test_benchmark(self) -> None:
        print("Benchmarking client with async I/0...")
        print()
        stream_name = "benchmark-" + uuid.uuid4().hex
        print("Warming up...")
        print()
        await self.write_events(stream_name, 1, 3)
        await self.read_events(stream_name, 1)
        print()
        for num_bytes in NUM_BYTES:
            print()
            print("Events with", num_bytes, "bytes of data...")
            print()
            for num_events in NUM_EVENTS:
                stream_name = "benchmark-" + uuid.uuid4().hex
                await self.write_events(stream_name, num_events, num_bytes)
                await self.read_events(stream_name, num_events)
                print()

    async def write_events(
        self, stream_name: str, num_events: int, num_bytes: int
    ) -> None:
        # print("Writing", num_events, "events with", num_bytes, "bytes of data...")
        start = datetime.now()
        for i in range(num_events):
            new_event = NewEvent(type="BenchmarkEvent", data=random_data(num_bytes))
            await self.client.append_events(
                stream_name,
                current_version=StreamState.NO_STREAM if not i else i - 1,
                events=[new_event],
            )
        end = datetime.now()
        duration = (end - start).total_seconds()
        print(
            "Wrote",
            num_events,
            "events in:",
            duration,
            "seconds,",
            num_events / duration,
            "events/s",
        )

    async def read_events(self, stream_name: str, num_events: int) -> None:
        # print("Reading", num_events, "events...")
        start = datetime.now()
        i = 0
        async for _ in await self.client.read_stream(stream_name):
            i += 1
        assert i == num_events
        end = datetime.now()
        duration = (end - start).total_seconds()
        print(
            "Read",
            num_events,
            "events in: ",
            duration,
            "seconds,",
            num_events / duration,
            "events/s",
        )


if __name__ == "__main__":
    unittest.main()
