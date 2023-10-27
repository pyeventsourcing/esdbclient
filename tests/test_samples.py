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
            sys.stderr.write(f'File "{file_path}", ')

            with open(file_path) as f:
                readlines = f.readlines()
                sys.stderr.write(f" {len(readlines)} lines")
                source = "".join(readlines)

            try:
                try:
                    compiled = compile(source=source, filename=file_path, mode="exec")
                finally:
                    sys.stderr.write(f" [@{get_elapsed_time()}]")
                    sys.stderr.flush()
                exec(compiled, globals(), globals())
                tb = None
            except Exception:
                lines = [
                    line
                    for line in traceback.format_exception(*sys.exc_info())
                    if "test_samples.py" not in line
                ]
                tb = "".join(lines)

            duration = get_duration()
            sys.stderr.write(f" [+{duration}]")

            if tb is not None:
                sample_failures += 1
                sys.stderr.write(f" ERROR: RAISED EXCEPTION\n\n{tb}\n")
            elif duration == "0.000s":
                sample_failures += 1
                sys.stderr.write(" ERROR: EXECUTED IN ZERO TIME\n\n")
            else:
                sys.stderr.write(" OK\n")
            sys.stderr.flush()

        sleep(0.1)
        if sample_failures:
            self.fail(f"{sample_failures} samples failed")
