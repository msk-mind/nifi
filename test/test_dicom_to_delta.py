#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nifi` package."""

import pytest
from unittest import TestCase, mock
from click.testing import CliRunner

import os
import sys
sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), '../src/') ))
from dicom_to_delta import *
from spark_session import SparkConfig
from pyspark.sql import SparkSession

SPARK = "local[*]"
DRIVER = "127.0.0.1"
HDFS = "" # use local fs
HDFS_PATH = "file:///tmp/test/"
DELTA_TABLE_PATH = "file:///tmp/dicom_proxy/"

BINARY_TABLE = os.path.join(DELTA_TABLE_PATH, "radiology.dcm_binary")
DCM_TABLE = os.path.join(DELTA_TABLE_PATH, "radiology.dcm")
OP_TABLE = os.path.join(DELTA_TABLE_PATH, "radiology.dcm_op")

class TestDicomToDelta(TestCase):

	def setUp(self):
		self.mock_spark = mock.Mock()

	def test_write_to_delta(self):

		self.mock_spark.return_value = SparkSession

		write_to_delta(self.mock_spark, HDFS, HDFS_PATH, DELTA_TABLE_PATH, False, False)

		assert 1 == self.mock_spark.sql.call_count

	def test_write_to_delta_merge(self):

		self.mock_spark.return_value = SparkSession

		write_to_delta(self.mock_spark, HDFS, HDFS_PATH, DELTA_TABLE_PATH, True, False)

		assert 1 == self.mock_spark.sql.call_count

	def test_write_to_delta_purge(self):

		self.mock_spark.return_value = SparkSession.builder \
			.appName("dicom-to-delta") \
			.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
			.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
		    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
		    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
			.getOrCreate()

		write_to_delta(self.mock_spark, HDFS, HDFS_PATH, DELTA_TABLE_PATH, False, True)

		assert 1 == self.mock_spark.sql.call_count
		assert 4 == self.mock_spark.conf.set.call_count


	def test_cli_missing_required_param(self):

		runner = CliRunner()

		result = runner.invoke(cli, ["-d", DRIVER, "-h", HDFS, "-r", HDFS_PATH, "-w", DELTA_TABLE_PATH])

		assert 2 == result.exit_code
		assert None != result.exception

	def test_cli_merge_and_purge(self):

		runner = CliRunner()

		result = runner.invoke(cli, ["-s", SPARK, "-d", DRIVER, "-h", HDFS, "-r", HDFS_PATH, "-w", DELTA_TABLE_PATH, "--merge", "--purge"])

		assert 1 == result.exit_code
		assert None != result.exception

