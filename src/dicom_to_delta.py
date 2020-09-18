import click
from pyspark.sql import SparkSession
from spark_session import *
import re

# Run Script: python3 dicom_to_delta.py -s <spark_master> -h <hdfs> -r <hdfs_path> -w <delta_table_path>

def create_delta_table(df, table_name, delta_path, merge, purge):
	"""
	Create delta table from the dataframe.
	"""
	# TODO repartition # based on the number of rows/performance
	# TODO upsert - do we still need merge?
	#   dcm: update on match AccessionNumber|AcquisitionNumber|SeriesNumber|InstanceNumber, otherwise insert
	#   binary: ...later for when we embed it in parquet
	#   op: update on match filename, otherwise insert

	if purge:
		# Changing a column's type or name or dropping a column requires rewriting the table.
		df.coalesce(128) \
			.write \
			.format("delta") \
			.option("overwriteSchema", "true") \
			.mode("overwrite") \
			.save(delta_path)
	if merge:
		# Adding new columns can be achived with .option("mergeSchema", "true")
		df.coalesce(128) \
			.write \
			.format("delta") \
			.option("mergeSchema", "true") \
			.mode("append") \
			.save(delta_path)

	else:
		df.coalesce(128) \
			.write \
			.format("delta") \
			.mode("append") \
			.save(delta_path)



def remove_delta_table(spark, table_name, delta_path):
	"""
	Clean up an existing delta table.
	"""
	from delta.tables import DeltaTable

	dt = DeltaTable.forPath(spark, delta_path)
	# delete latest version
	dt.delete()
	# Disable check for retention - default 136 hours (7 days)
	spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
	dt.vacuum(0) # vacuum all data


@click.command()
@click.option("-s", "--spark", required=True, help="Spark master string e.g. spark://master_ip:7077")
@click.option("-d", "--driver", default="127.0.0.1", show_default=True, help="Driver Host IP")
@click.option("-h", "--hdfs", default="hdfs://sandbox-hdp.hortonworks.com:8020/", show_default=True, help="HDFS uri e.g. hdfs://sandbox-hdp.hortonworks.com:8020/")
@click.option("-r", "--read", required=True, help="Parquet file directory to read from")
@click.option("-w", "--write", required=True, help="Delta table write directory")
# Note: use with caution. Either use merge or purge, not both.
@click.option("-m", "--merge", is_flag=True, default=False, show_default=True, help="(optional) Merge schema - add new columns")
@click.option("-p", "--purge", is_flag=True, default=False, show_default=True, help="(optional) Delete all delta tables - then create new tables")
def cli(spark, driver, hdfs ,read, write, merge, purge):
	"""
	Main CLI - setup spark session and call write to delta.
	"""
	if merge and purge:
		raise ValueError("Cannot use flags merge and purge at the same time!")

	sc = SparkConfig()
	spark_session = sc.spark_session(spark, driver)

	write_to_delta(spark_session, hdfs ,read, write, merge, purge)


def write_to_delta(spark, hdfs ,read, write, merge, purge):
	"""
	Create proxy tables from Dicom parquet files
	"""
	# input file path
	common_hdfs_path = hdfs + read
	binary_path = common_hdfs_path + "/dicom"
	dcm_path = common_hdfs_path + "/parquet/*.dicom.parquet"
	op_path = common_hdfs_path + "/parquet/*.metadata.parquet"

	# table name
	binary_table = "radiology.dcm_binary"
	dcm_table = "radiology.dcm"
	op_table = "radiology.dcm_op"

	# output delta table path
	common_delta_path = "".join((hdfs, write, "/"))
	binary_delta_path = common_delta_path + binary_table
	dcm_delta_path = common_delta_path + dcm_table
	op_delta_path = common_delta_path + op_table

	# Read dicom files
	binary_df = spark.read.format("binaryFile") \
		.option("pathGlobFilter", "*.dcm") \
		.option("recursiveFileLookup", "true") \
		.load(binary_path)

	# Read parquet files
	dcm_df = spark.read.parquet(dcm_path)
	op_df = spark.read.parquet(op_path)

	# To improve read performance when you load data back, Databricks recommends 
	# turning off compression when you save data loaded from binary files:
	spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

	# clean up the latest delta table version
	if purge:
		remove_delta_table(spark, binary_table, binary_delta_path)
		remove_delta_table(spark, dcm_table, dcm_delta_path)
		remove_delta_table(spark, op_table, op_delta_path)

	# Create Delta tables
	spark.sql("CREATE DATABASE IF NOT EXISTS radiology")
	create_delta_table(binary_df, binary_table, binary_delta_path, merge, purge)
	create_delta_table(dcm_df, dcm_table, dcm_delta_path, merge, purge)

	# Filter duplicate attributes created in nifi that endswith _#
	dcm_op = op_df.drop(*filter(lambda col: re.search(r'_\d', col), op_df.columns))
	create_delta_table(dcm_op, op_table, op_delta_path, merge, purge)


if __name__ == '__main__':
	cli()

