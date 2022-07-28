#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `speasy.core.cdf` package."""
import unittest
from ddt import ddt, data, unpack
import requests
import speasy.core.cdf

try:
    import spacepy.pycdf
except ImportError:
    raise unittest.SkipTest("Missing spacepy.pycdf")


@ddt
class TestISTPLoader(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
