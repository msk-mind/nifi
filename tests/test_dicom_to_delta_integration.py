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

SPARK = "local[2]"
DRIVER = "127.0.0.1"
HDFS = "" # use local fs
DATASET_ADDRESS = "./tests/testdata/test_dataset_address"
DELTA_TABLE_PATH = os.path.join(DATASET_ADDRESS, "table")

BINARY_TABLE = os.path.join(DELTA_TABLE_PATH, "dicom_binary")
DCM_TABLE = os.path.join(DELTA_TABLE_PATH, "dicom")
TABLES = [BINARY_TABLE, DCM_TABLE]


@pytest.fixture(autouse=True)
def spark():
    print('------setup------')
    spark = SparkConfig().spark_session(SPARK, DRIVER, fs=True)
    yield spark

    print('------teardown------')
    for table_path in TABLES:
        if os.path.exists(table_path):
            shutil.rmtree(table_path)

def assertions(spark):
    for table in TABLES:
        df = spark.read.format("delta").load(table)
        assert 3 == df.count()


def test_write_to_delta(spark):

	write_to_delta(spark, HDFS, DATASET_ADDRESS, False, False)
	assertions(spark)



def test_write_to_delta_merge(spark):

	write_to_delta(spark, HDFS, DATASET_ADDRESS, True, False)
	assertions(spark)
