# run_tests.py
import os
import sys
import unittest
from typing import cast

import coverage
from coverage.results import display_covered, should_fail_under

nocover_tags = []

if "26.1" in os.getenv("KURRENTDB_DOCKER_IMAGE", ""):
    pass
elif "26.0" in os.getenv("KURRENTDB_DOCKER_IMAGE", ""):
    nocover_tags.append(r"<26\.1")
elif "25.1" in os.getenv("KURRENTDB_DOCKER_IMAGE", ""):
    nocover_tags.append(r"<26\.1")
    nocover_tags.append(r"<26\.0")
else:
    nocover_tags.append(r"<26\.1")
    nocover_tags.append(r"<26\.0")
    nocover_tags.append(r"<25\.1")

cov = coverage.Coverage()
exclude_lines = cov.get_option("report:exclude_lines") or []
print("Coverage exclude_lines:")
for tag in nocover_tags:
    exclude_lines += [
        rf"pragma: {tag} no cover",
        rf"no cover {tag}: start(?s:.)*?no cover {tag}: stop",
    ]
for exclude_line in exclude_lines:
    print(" - ", exclude_line)
cov.set_option("report:exclude_lines", exclude_lines)
cov.start()

# Run tests
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(unittest.defaultTestLoader.discover("tests"))

cov.stop()
cov.save()

if result.testsRun == 0 and len(result.skipped) == 0:
    sys.exit(1)
elif result.wasSuccessful():
    total = cov.report()

    fail_under = cast(float, cov.get_option("report:fail_under"))
    precision = cast(int, cov.get_option("report:precision"))
    if should_fail_under(total, fail_under, precision):
        msg = "total of {total} is less than fail-under={fail_under:.{p}f}".format(
            total=display_covered(total, precision),
            fail_under=fail_under,
            p=precision,
        )
        print("Coverage failure:", msg)
        sys.exit(2)

    sys.exit(0)
else:
    sys.exit(1)
