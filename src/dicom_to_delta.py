import click
from pyspark.sql import SparkSession
from spark_session import *


# Run Script: python3 dicom_to_delta.py--spark <spark_master> --hdfs <hdfs_ip> --read <hdfs_path> --write <delta_table_path>

def create_delta_table(spark, df, table_name, delta_path, merge, purge):
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
@click.option("-h", "--hdfs", required=True, help="HDFS IP")
@click.option("-r", "--read", required=True, help="Parquet file directory to read from")
@click.option("-w", "--write", required=True, help="Delta table write directory")
@click.option("-m", "--merge", type=bool, default=False, show_default=True, help="(optional) Merge schema - add new columns")
@click.option("-p", "--purge", type=bool, default=False, show_default=True, help="(optional) Delete all delta tables - then create new tables")
def write_to_delta(spark, driver, hdfs ,read, write, merge, purge):
	"""
	Create proxy tables from Dicom parquet files
	"""

	spark = spark_session(spark, driver)

	# input file path
	common_hdfs_path = "".join(("hdfs://", hdfs, ":8020/", read))
	binary_path = common_hdfs_path + "/dicom"
	dcm_path = common_hdfs_path + "/parquet/*.dicom.parquet"
	op_path = common_hdfs_path + "/parquet/*.metadata.parquet"

	# table name
	binary_table = "radiology.dcm_binary"
	dcm_table = "radiology.dcm"
	op_table = "radiology.dcm_op"

	# output delta table path
	common_delta_path = "".join(("hdfs://", hdfs, ":8020/", write, "/"))
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
	create_delta_table(spark, binary_df, binary_table, binary_delta_path, merge, purge)
	create_delta_table(spark, dcm_df, dcm_table, dcm_delta_path, merge, purge)
	create_delta_table(spark, op_df, op_table, op_delta_path, merge, purge)


if __name__ == '__main__':
	write_to_delta()

