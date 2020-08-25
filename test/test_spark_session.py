import pytest

import os
import sys
sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), '../src/') ))
from spark_session import SparkConfig
from pyspark.sql import SparkSession

SPARK = "local[*]"
DRIVER = "127.0.0.1"

def test_spark_session():

	spark = SparkConfig().spark_session(SPARK, DRIVER)

	assert "true" == spark.conf.get("spark.hadoop.dfs.client.use.datanode.hostname")
	assert DRIVER == spark.conf.get("spark.driver.host")
	assert SPARK == spark.conf.get("spark.master")
	