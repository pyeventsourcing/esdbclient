# -*- coding: utf-8 -*-
import sys

if sys.version_info[0:2] > (3, 7):
    from unittest import IsolatedAsyncioTestCase
else:
    from async_case import IsolatedAsyncioTestCase


class TestAsyncSetupError(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        raise Exception("This should cause the test suite to fail")

    async def test(self):
        pass


class TestAsyncTeardownError(IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        raise Exception("This should cause the test suite to fail")

    async def test(self):
        pass


class TestAsyncTestError(IsolatedAsyncioTestCase):
    async def test(self):
        raise Exception("This should cause the test suite to fail")
