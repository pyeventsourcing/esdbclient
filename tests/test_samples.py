# -*- coding: utf-8 -*-
import os
import sys
import traceback
from pathlib import Path
from time import sleep
from unittest import TestCase

from tests.test_client import get_duration, get_elapsed_time


class TestSamples(TestCase):
    def test(self) -> None:
        # Gather samples.
        base_dir = Path(__file__).parents[1]
        samples_path = os.path.join(base_dir, "samples")
        file_paths = []
        for dirpath, _, filenames in os.walk(samples_path):
            for filename in filenames:
                if filename.endswith(".py"):
                    file_path = os.path.join(samples_path, dirpath, filename)
                    file_paths.append(file_path)
                    sys.stdout.write(file_path)
        sample_failures = 0

        # Exec each sample.
        for file_path in file_paths:
            # Copy os.environ.
            oringinal_env = os.environ.copy()

            # Print out the sample file path.
            sys.stderr.write(f'File "{file_path}", ')

            # Read the sample.
            with open(file_path) as f:
                readlines = f.readlines()
                sys.stderr.write(f" {len(readlines)} lines")
                source = "".join(readlines)

            # Compile and exec the sample.
            try:
                try:
                    compiled = compile(
                        source,
                        filename=file_path,
                        mode="exec",
                    )
                finally:
                    # Track elapsed time of the test suite.
                    sys.stderr.write(f" [@{get_elapsed_time()}]")
                    sys.stderr.flush()
                exec(compiled, {"__name__": "__main__"}, {})
                tb = None
            except Exception:
                lines = [
                    line
                    for line in traceback.format_exception(*sys.exc_info())
                    if "test_samples.py" not in line
                ]
                tb = "".join(lines)

            # Track duration of executing the sample.
            duration = get_duration()
            sys.stderr.write(f" [+{duration}]")

            # Check the sample executed okay.
            if tb is not None:
                sample_failures += 1
                sys.stderr.write(f" ERROR: RAISED EXCEPTION\n\n{tb}\n")
            elif duration == "0.000s":
                sample_failures += 1
                sys.stderr.write(" ERROR: EXECUTED IN ZERO TIME\n\n")
            else:
                sys.stderr.write(" OK\n")
            sys.stderr.flush()

            # Restore os.environ.
            for key, value in os.environ.items():
                if key not in oringinal_env:
                    os.environ.pop(key)
                else:
                    os.environ[key] = value

        # Allow time for the above writes to flush.
        sleep(0.1)

        # Check for any sample failures.
        if sample_failures:
            self.fail(f"{sample_failures} samples failed")
