#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nifi` package.

To run

cd nifi
pytest -s test/test_dicom_to_delta_integration.py
"""

import pytest
from click.testing import CliRunner

import os, sys, shutil
sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), '../src/') ))
from dicom_to_delta import *
from spark_session import SparkConfig
from pyspark.sql import SparkSession

SPARK = "local[*]"
DRIVER = "127.0.0.1"
HDFS = "" # use local fs
HDFS_PATH = "./test/testdata/"
DELTA_TABLE_PATH = "./test/testdata/proxytable"

BINARY_TABLE = os.path.join(DELTA_TABLE_PATH, "radiology.dcm_binary")
DCM_TABLE = os.path.join(DELTA_TABLE_PATH, "radiology.dcm")
OP_TABLE = os.path.join(DELTA_TABLE_PATH, "radiology.dcm_op")
TABLES = [BINARY_TABLE, DCM_TABLE, OP_TABLE]


@pytest.fixture(autouse=True)
def cleanup():
	for table in TABLES:
		if os.path.exists(table):
			shutil.rmtree(table)


def assertions(spark):
	for table in TABLES:
		df = spark.read.format("delta").load(table)
		assert 3 == df.count()


def test_write_to_delta(spark_session):

	write_to_delta(spark_session, HDFS, HDFS_PATH, DELTA_TABLE_PATH, False, False)
	assertions(spark_session)



def test_write_to_delta_merge(spark_session):

	write_to_delta(spark_session, HDFS, HDFS_PATH, DELTA_TABLE_PATH, True, False)
	assertions(spark_session)

