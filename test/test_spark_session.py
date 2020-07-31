import unittest
import pytest
from pyspark.sql import SparkSession

class TestPySpark:
	
	@classmethod
	def create_pyspark_session(cls):
		return (SparkSession.builder \
			.appName('test') \
			.getOrCreate())
	 
	@classmethod
	def setUpClass(cls):
		cls.spark = cls.create_pyspark_session()
	
	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()