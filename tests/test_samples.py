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
        samples_dir = base_dir / "samples"
        sample_files = []
        for dirpath, _, filenames in os.walk(samples_dir):
            for filename in filenames:
                if filename.endswith(".py"):
                    sample_file = samples_dir / dirpath / filename
                    sample_files.append(sample_file)

        if len(sample_files) == 0:
            self.fail(f"No sample files found on path: {samples_dir}")
        else:
            sys.stderr.write("\n")

        # Run each sample...
        sample_failures = 0
        for sample_file in sample_files:
            # Copy os.environ.
            oringinal_env = os.environ.copy()

            # Print out the sample file path.
            sys.stderr.write(f'File "{sample_file}"')

            # Read the sample.
            with open(sample_file) as f:
                sample_lines = f.readlines()
                sys.stderr.write(f" has {len(sample_lines)} lines")
                source = "".join(sample_lines)

            # Compile and exec the sample.
            try:
                try:
                    compiled = compile(
                        source,
                        filename=sample_file,
                        mode="exec",
                    )
                finally:
                    # Track elapsed time of the test suite.
                    sys.stderr.write(f" [@{get_elapsed_time()}]")
                    sys.stderr.flush()
                exec(compiled, {"__name__": "__main__"}, {})
                tb = None
            except Exception:
                # Remove this call from the traceback (so it's just clearer).
                tb_lines = [
                    line
                    for line in traceback.format_exception(*sys.exc_info())
                    if "test_samples.py" not in line
                ]
                tb = "".join(tb_lines)

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

        # Assert there were zero sample failures.
        if sample_failures != 0:
            self.fail(f"{sample_failures} samples failed")
