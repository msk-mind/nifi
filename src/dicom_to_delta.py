import argparse
from pyspark.sql import SparkSession

# Run Script: python3 dicom_to_delta.py--spark <spark_master> --hdfs <hdfs_ip> --read <hdfs_path> --write <delta_table_path>
class DicomToDelta:

	def __init__(self, spark_master_uri):

		# Setup spark session
		self.spark = SparkSession.builder \
				.appName("dicom-to-delta") \
				.master(spark_master_uri) \
				.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
				.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
			    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
			    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
				.config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
				.config("spark.driver.port", "8001") \
				.config("spark.blockManager.port", "8002") \
				.config("spark.driver.host", "127.0.0.1") \
				.config("spark.driver.bindAddress", "0.0.0.0") \
				.getOrCreate()


	def create_delta_table(self, df, table_name, delta_path, merge, purge):
		"""
		Create delta table from the dataframe.
		"""
		if merge:
			# Adding new columns can be achived with .option("mergeSchema", "true")
			df.repartition(1) \
				.write \
				.format("delta") \
				.option("mergeSchema", "true") \
				.mode("append") \
				.save(delta_path)
		elif purge:
			# Changing a column's type or name or dropping a column requires rewriting the table.
			df.repartition(1) \
				.write \
				.format("delta") \
				.option("overwriteSchema", "true") \
				.mode("append") \
				.save(delta_path)
		else:
			df.repartition(1) \
				.write \
				.format("delta") \
				.mode("append") \
				.save(delta_path)



	def clean_up(self, table_name, delta_path):
		"""
		Clean up an existing delta table
		"""
		from delta.tables import DeltaTable

		dt = DeltaTable.forPath(self.spark, delta_path)
		# delete latest version
		dt.delete()
		# Disable check for retention - default 136 hours (7 days)
		self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
		dt.vacuum(0) # vacuum all data
	

	def write_to_delta(self, hdfs_ip ,hdfs_path, delta_table_path, merge, purge):

		# input file path
		common_hdfs_path = "".join(("hdfs://", hdfs_ip, ":8020/", hdfs_path))
		binary_path = common_hdfs_path + "/dicom"
		dcm_path = common_hdfs_path + "/parquet/*.dicom.parquet"
		op_path = common_hdfs_path + "/parquet/*.metadata.parquet"

		# table name
		binary_table = "radiology.dcm_binary"
		dcm_table = "radiology.dcm"
		op_table = "radiology.dcm_op"

		# output delta table path
		common_delta_path = "".join(("hdfs://", hdfs_ip, ":8020/", delta_table_path, "/"))
		binary_delta_path = common_delta_path + binary_table
		dcm_delta_path = common_delta_path + dcm_table
		op_delta_path = common_delta_path + op_table

		# Read dicom files
		binary_df = self.spark.read.format("binaryFile") \
			.option("pathGlobFilter", "*.dcm") \
			.option("recursiveFileLookup", "true") \
			.load(binary_path)

		# Read parquet files
		dcm_df = self.spark.read.parquet(op_path)
		op_df = self.spark.read.parquet(dcm_path)

		# To improve read performance when you load data back, Databricks recommends 
		# turning off compression when you save data loaded from binary files:
		self.spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

		# clean up the latest delta table version
		if purge:
			self.clean_up(binary_table, binary_delta_path)
			self.clean_up(dcm_table, dcm_delta_path)
			self.clean_up(op_table, op_delta_path)

		# Create Delta tables
		self.spark.sql("CREATE DATABASE IF NOT EXISTS radiology")
		self.create_delta_table(binary_df, binary_table, binary_delta_path, merge, purge)
		self.create_delta_table(dcm_df, dcm_table, dcm_delta_path, merge, purge)
		self.create_delta_table(op_df, op_table, op_delta_path, merge, purge)


if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='Create proxy tables from Dicom parquet files.')
	parser.add_argument('--spark', type=str, required=True, help='Spark master string e.g. spark://master_ip:7077')
	parser.add_argument('--hdfs', type=str, required=True, help='HDFS IP')
	parser.add_argument('--read', type=str, required=True, help='Parquet file directory to read from')
	parser.add_argument('--write', type=str, required=True, help='Delta table write directory')
	# NOTE: Use either merge or purge
	parser.add_argument('--merge', dest='merge', action='store_true', help='(optional) Merge schema - add new columns')
	parser.add_argument('--purge', dest='purge', action='store_true', help='(optional) Delete all delta tables - then create new tables')

	args = parser.parse_args()
	print(args)

	DicomToDelta(args.spark).write_to_delta(args.hdfs, args.read, args.write, args.merge, args.purge)

