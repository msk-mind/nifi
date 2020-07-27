import argparse
from pyspark.sql import SparkSession

# Run Script: python3 dicom_to_delta.py--spark <spark_master_ip> --hdfs <hdfs_ip> --read <hdfs_path> --write <delta_table_path>

def create_delta_table(df, table_name, delta_path, merge, purge):
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

	# create table in metastore
	df.repartition(1) \
		.write \
		.format("delta") \
		.mode("overwrite") \
		.saveAsTable(table_name)


def clean_up(spark, table_name, delta_path):
	"""
	Clean up an existing delta table
	"""
	from delta.tables import DeltaTable

	dt = DeltaTable.forPath(spark, delta_path)
	# delete latest version
	dt.delete()
	# Disable check for retention - default 136 hours (7 days)
	spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
	dt.vacuum(0) # vacuum all data
	
	spark.sql("DROP TABLE IF EXISTS " + table_name)


def write_to_delta(spark_master, hdfs_ip ,hdfs_path, delta_table_path, merge, purge):

	# Setup spark context
	master_uri = "spark://" + spark_master + ":7077"

	# NOTE: master() can be commented out to run script without a spark cluster
	spark = SparkSession.builder \
			.appName("dicom-to-delta") \
			.master(master_uri) \
			.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
			.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
		    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
		    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
			.config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
			.config("spark.driver.host", "127.0.0.1") \
			.config("spark.driver.port", "8001") \
			.config("spark.blockManager.port", "8002") \
			.config("spark.driver.bindAddress", "0.0.0.0") \
			.config("spark.cores.max", "6") \
			.getOrCreate()

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
	binary_df = spark.read.format("binaryFile") \
		.option("pathGlobFilter", "*.dcm") \
		.option("recursiveFileLookup", "true") \
		.load(binary_path)

	# Read parquet files
	dcm_df = spark.read.parquet(op_path)
	op_df = spark.read.parquet(dcm_path)

	# To improve read performance when you load data back, Databricks recommends 
	# turning off compression when you save data loaded from binary files:
	spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

	# clean up the latest delta table version
	if purge:
		clean_up(spark, binary_table, binary_delta_path)
		clean_up(spark, dcm_table, dcm_delta_path)
		clean_up(spark, op_table, op_delta_path)

	# Create Delta tables
	spark.sql("CREATE DATABASE IF NOT EXISTS radiology")
	create_delta_table(binary_df, binary_table, binary_delta_path, merge, purge)
	create_delta_table(dcm_df, dcm_table, dcm_delta_path, merge, purge)
	create_delta_table(op_df, op_table, op_delta_path, merge, purge)

	spark.stop()


if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='Create proxy tables from Dicom parquet files.')
	parser.add_argument('--spark', type=str, required=True, help='Spark master IP')
	parser.add_argument('--hdfs', type=str, required=True, help='HDFS IP')
	parser.add_argument('--read', type=str, required=True, help='Parquet file directory to read from')
	parser.add_argument('--write', type=str, required=True, help='Delta table write directory')
	# NOTE: Use either merge or purge
	parser.add_argument('--merge', dest='merge', action='store_true', help='(optional) Merge schema - add new columns')
	parser.add_argument('--purge', dest='purge', action='store_true', help='(optional) Delete all delta tables - then create new tables')

	args = parser.parse_args()
	print(args)
	write_to_delta(args.spark, args.hdfs, args.read, args.write, args.merge, args.purge)

