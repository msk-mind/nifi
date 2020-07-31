#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nifi` package."""

from test_spark_session import TestPySpark
import sys
sys.path.append('../src')
from dicom_to_delta import *

class TestDicomToDelta(TestPySpark):

	def test_create_delta_table(self):
		
		assert True

