#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nifi` package."""

import pytest
from click.testing import CliRunner

import sys
sys.path.append('../src')

from dicom_to_delta import write_to_delta
from spark_session import *
from pyspark.sql import SparkSession

SPARK = "local[*]"
DRIVER = "127.0.0.1"
HDFS_IP = "sandbox-hdp.hortonworks.com" # map to your hdfs in /etc/hosts
HDFS_PATH = "/db/test/"
DELTA_TABLE_PATH = "/tmp/dicom_proxy/"

BINARY_TABLE = "".join(("hdfs://", HDFS_IP, ":8020/", DELTA_TABLE_PATH, "radiology.dcm_binary"))
DCM_TABLE = "".join(("hdfs://", HDFS_IP, ":8020/", DELTA_TABLE_PATH, "radiology.dcm"))
OP_TABLE = "".join(("hdfs://", HDFS_IP, ":8020/", DELTA_TABLE_PATH, "radiology.dcm_op"))

runner = CliRunner()


def test_write_to_delta():
	
	result = runner.invoke(write_to_delta, 
		['-s',SPARK, '-d',DRIVER, '-h',HDFS_IP, '-r',HDFS_PATH, '-w',DELTA_TABLE_PATH, '-m',False, '-p',False])

	assert result.exit_code == 0
	assert result.exception == None

def test_write_to_delta_merge():

	result = runner.invoke(write_to_delta,
		['-s',SPARK, '-d',DRIVER, '-h',HDFS_IP, '-r',HDFS_PATH, '-w',DELTA_TABLE_PATH, '-m',True, '-p',False])

	assert result.exit_code == 0
	assert result.exception == None


def test_write_to_delta_purge():
	
	result = runner.invoke(write_to_delta,
		['-s',SPARK, '-d',DRIVER, '-h',HDFS_IP, '-r',HDFS_PATH, '-w',DELTA_TABLE_PATH, '-m',False, '-p',True])

	assert result.exit_code == 0
	assert result.exception == None
