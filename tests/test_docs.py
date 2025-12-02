from __future__ import annotations

import ast
import asyncio
import contextlib
import os
import ssl
from pathlib import Path

# from tempfile import NamedTemporaryFile
from unittest import TestCase
from uuid import uuid4

from tests.test_client import SERVER_VERSION

BASE_DIR = Path(__file__).parents[1]


class TestDocs(TestCase):
    def setUp(self) -> None:
        self.setup_environ()
        # try:
        #     loop = asyncio.get_event_loop()
        # except RuntimeError as e:
        #     if str(e).startswith("There is no current event loop in thread"):
        #         loop = asyncio.new_event_loop()
        #         asyncio.set_event_loop(loop)
        #     else:
        #         raise

    def setup_environ(self) -> None:
        os.environ["KDB_ROOT_CERTIFICATES"] = ssl.get_server_certificate(
            addr=("localhost", 2114)
        )
        os.environ["KDB_URI"] = "kdb://admin:changeit@localhost:2114"

    def tearDown(self) -> None:
        del os.environ["KDB_URI"]
        with contextlib.suppress(KeyError):
            del os.environ["KDB_ROOT_CERTIFICATES"]

    def test_readme(self) -> None:
        self._out = ""

        path = BASE_DIR / "README.md"
        if not path.exists():
            self.fail(f"README file not found: {path}")
        self.check_code_snippets_in_file(path)

    def test_vuepress_docs(self) -> None:
        docs_path = BASE_DIR / "docs" / "api"
        docs_paths = list(docs_path.glob("**/*"))
        for doc_path in docs_paths:
            if (
                "getting-started" in doc_path.name
                or "appending-events" in doc_path.name
                or "reading-events" in doc_path.name
            ):
                print()
                print("Test vuepress docs sync code examples in", doc_path.name)
                print()
                self.check_code_snippets_in_file(doc_path, sync_only=True)
                print()
                print("Test vuepress docs async code examples in", doc_path.name)
                print()
                self.check_code_snippets_in_file(doc_path, async_only=True)
            else:
                continue

    def check_code_snippets_in_file(
        self, doc_path: Path, *, sync_only: bool = False, async_only: bool = False
    ) -> None:
        # Extract lines of Python code from the README.md file.

        replacements = {
            "order:123": f"order:123-{uuid4()}",
            "order:456": f"order:456-{uuid4()}",
        }

        print_block_line_numbers = True
        lines = ["import sys"] if print_block_line_numbers else []
        num_code_lines = 0
        num_code_lines_in_block = 0
        is_code = False
        is_tabs = False
        is_sync_tab = False
        is_async_tab = False
        is_md = False
        is_rst = False
        is_ignoring_remainder_of_code_in_block = False
        last_line = ""
        is_literalinclude = False
        with doc_path.open() as doc_file:
            for line_index, orig_line in enumerate(doc_file, start=-len(lines)):
                line = orig_line.strip("\n")
                if line.startswith("```python") and not (
                    (sync_only and is_async_tab) or (async_only and is_sync_tab)
                ):
                    # Start markdown code block.
                    if is_rst:
                        self.fail(
                            "Markdown code block found after restructured text block "
                            "in same file."
                        )
                    is_code = True
                    is_md = True
                    if print_block_line_numbers:
                        line = (
                            f"sys.stderr.write('Block on line: {line_index}\\n') and"
                            " sys.stderr.flush()"
                        )
                    else:
                        line = ""
                    num_code_lines_in_block = 0
                elif is_code and is_md and line.startswith("```"):
                    # Finish markdown code block.
                    if not num_code_lines_in_block:
                        self.fail(f"No lines of code in block: {line_index + 1}")
                    is_code = False
                    is_ignoring_remainder_of_code_in_block = False
                    line = ""
                elif is_code and is_rst and line.startswith("```"):
                    # Can't finish restructured text block with markdown.
                    self.fail(
                        "Restructured text block terminated with markdown format '```'"
                    )
                elif (
                    line.startswith(".. code:: python")
                    or line.strip() == ".."
                    # and "exclude-when-testing" not in last_line
                ):
                    # Start restructured text code block.
                    if is_md:
                        self.fail(
                            "Restructured text code block found after markdown block "
                            "in same file."
                        )
                    is_code = True
                    is_rst = True
                    line = ""
                    num_code_lines_in_block = 0
                elif line.startswith(".. literalinclude::"):
                    is_literalinclude = True
                    line = ""

                elif is_literalinclude:
                    if "pyobject" in line:
                        # Assume ".. literalinclude:: ../../xxx/xx.py"
                        # Or ".. literalinclude:: ../xxx/xx.py"
                        module = last_line.strip().split(" ")[-1][:-3]
                        module = module.lstrip("./")
                        module = module.replace("/", ".")
                        # Assume "    :pyobject: xxxxxx"
                        pyobject = line.strip().split(" ")[-1]
                        statement = f"from {module} import {pyobject}"
                        line = statement
                        is_literalinclude = False

                elif is_code and is_rst and line and not line.startswith(" "):
                    # Finish restructured text code block.
                    if not num_code_lines_in_block:
                        self.fail(f"No lines of code in block: {line_index + 1}")
                    is_code = False
                    line = ""
                elif is_code:
                    # Process line in code block.
                    # Restructured code block normally indented with four spaces.
                    if is_rst and len(line.strip()):
                        if not line.startswith("    "):
                            self.fail(
                                f"Code line needs 4-char indent: {line!r}: "
                                f"{doc_path}"
                            )
                        # Strip four chars of indentation.
                        line = line[4:]

                    # Exclude version-specific unsupported code.
                    if SERVER_VERSION < (25, 1) and "multi_append_to_stream" in line:
                        is_ignoring_remainder_of_code_in_block = True

                    if is_ignoring_remainder_of_code_in_block:
                        line = ""
                    elif len(line.strip()):
                        num_code_lines_in_block += 1
                        num_code_lines += 1

                elif line.startswith("::: tabs"):
                    is_tabs = True
                    line = ""

                elif is_tabs and line.startswith(":::"):
                    is_tabs = False
                    is_sync_tab = False
                    is_async_tab = False
                    line = ""

                elif is_tabs and line.startswith("@tab sync"):
                    is_sync_tab = True
                    is_async_tab = False
                    line = ""

                elif is_tabs and line.startswith("@tab async"):
                    is_sync_tab = False
                    is_async_tab = True
                    line = ""

                else:
                    line = ""

                if "get_server_certificate" in line:
                    line = line.replace("2113", "2114")

                # if "Block on line" not in line.strip() and line.strip():
                #     print(line)
                lines.append(line)
                last_line = orig_line

        print(f"{num_code_lines} lines of code in {doc_path}")

        for i, line in enumerate(lines):
            if "set_tracer_provider(" in line:
                lines[i] = ""
            if "instrument()" in line:
                lines[i] = ""
            if "uninstrument()" in line:
                lines[i] = ""

        source = "\n".join(lines) + "\n"

        for replacement in replacements.items():
            source = source.replace(replacement[0], replacement[1])

        # # Write the code into a temp file.
        # tempfile = NamedTemporaryFile("w+")
        # tempfile.writelines(source)
        # tempfile.flush()

        result = eval(  # noqa: S307, PGH001
            compile(
                source=source,
                filename=doc_path,
                mode="exec",
                flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
            ),
            globals(),
            globals(),
        )
        if asyncio.iscoroutine(result):
            asyncio.run(result)

        # print(Path.cwd())
        # print("\n".join(lines) + "\n")
        #
        # # Run the code and catch errors.
        # p = Popen(
        #     [sys.executable, temp_path],
        #     stdout=PIPE,
        #     stderr=PIPE,
        #     env={"PYTHONPATH": BASE_DIR},
        # )
        # print(sys.executable, temp_path, PIPE)
        # out, err = p.communicate()
        # decoded_out = out.decode("utf8").replace(temp_path, str(doc_path))
        # decoded_err = err.decode("utf8").replace(temp_path, str(doc_path))
        # exit_status = p.wait()
        #
        # print(decoded_out)
        # print(decoded_err)
        #
        # # Check for errors running the code.
        # if exit_status:
        #     self.fail(decoded_out + decoded_err)
        #
        # # Close (deletes) the tempfile.
        # tempfile.close()


class TestDocsInsecure(TestDocs):
    def setup_environ(self) -> None:
        os.environ["KDB_URI"] = "kdb://localhost:2113?Tls=false"
