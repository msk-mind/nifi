#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nifi` package."""

import logging
import pytest
import sys
sys.path.append('../src')

from dicom_to_delta import DicomToDelta
from pyspark.sql import SparkSession

d2d = DicomToDelta("local[*]")
HDFS_IP = "sandbox-hdp.hortonworks.com" # map to your hdfs in /etc/hosts
HDFS_PATH = "/db/test/"
DELTA_TABLE_PATH = "/tmp/dicom_proxy/"

BINARY_TABLE = "".join(("hdfs://", HDFS_IP, ":8020/", DELTA_TABLE_PATH, "radiology.dcm_binary"))
DCM_TABLE = "".join(("hdfs://", HDFS_IP, ":8020/", DELTA_TABLE_PATH, "radiology.dcm"))
OP_TABLE = "".join(("hdfs://", HDFS_IP, ":8020/", DELTA_TABLE_PATH, "radiology.dcm_op"))

from delta.tables import DeltaTable

def test_write_to_delta():
	
	d2d.write_to_delta(HDFS_IP ,HDFS_PATH, DELTA_TABLE_PATH, False, False)

	# read delta table
	dt = DeltaTable.forPath(d2d.spark, DCM_TABLE)
	assert dt.toDF().count() >= 14


def test_write_to_delta_merge():
	
	d2d.write_to_delta(HDFS_IP ,HDFS_PATH, DELTA_TABLE_PATH, True, False)

	# read delta table
	dt = DeltaTable.forPath(d2d.spark, OP_TABLE)
	assert dt.toDF().count() >= 14


def test_write_to_delta_purge():
	
	d2d.write_to_delta(HDFS_IP ,HDFS_PATH, DELTA_TABLE_PATH, False, True)

	# read delta table
	dcm = DeltaTable.forPath(d2d.spark, DCM_TABLE)
	assert dcm.toDF().count() == 14

	op = DeltaTable.forPath(d2d.spark, OP_TABLE)
	assert op.toDF().count() == 14