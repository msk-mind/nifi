import sys
from pyspark.sql import SparkSession

# Run Script: python3 dicom_to_delta.py <spark_master_ip> <hdfs_ip> <hdfs_path> <delta_table_path>


# TODO: error handling for updated/new schema.
# Adding new columns can be achived with .option("mergeSchema", "true")
# Changing a column’s type or name or dropping a column requires rewriting the table. .option("overwriteSchema", "true")
def create_delta_table(df, delta_path):
	"""
	Create delta table from the dataframe.
	"""
	df.repartition(1) \
		.write.format("delta") \
		.mode("overwrite") \
		.save(delta_path)


def write_to_delta(spark_master, hdfs_ip ,hdfs_path, delta_table_path):

	# Setup spark context
	master_uri = "spark://" + spark_master + ":7077"

	# master() can be commented out to run script without access to a spark cluster
	spark = SparkSession.builder \
			.appName("dicom-to-delta") \
			.master(master_uri) \
			.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
			.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
			.config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
			.config("spark.driver.host", "127.0.0.1") \
			.config("spark.driver.port", "8082") \
			.config("spark.blockManager.port", "8888") \
			.config("spark.driver.bindAddress", "0.0.0.0") \
			.config("spark.cores.max", "6") \
			.getOrCreate()
		

	# input file path
	dicom_file_path = "".join(("hdfs://", hdfs_ip, ":8020/", hdfs_path, "/dicom"))
	dcm_path = "".join(("hdfs://", hdfs_ip, ":8020/", hdfs_path, "/parquet/*.dicom.parquet"))
	op_path = "".join(("hdfs://", hdfs_ip, ":8020/", hdfs_path, "/parquet/*.metadata.parquet"))

	# output delta table path
	binary_delta_path = "".join(("hdfs://", hdfs_ip, ":8020/", delta_table_path, "/dcm_binary"))
	dcm_delta_path = "".join(("hdfs://", hdfs_ip, ":8020/", delta_table_path, "/dcm"))
	op_delta_path = "".join(("hdfs://", hdfs_ip, ":8020/", delta_table_path, "/dcm_op"))

	# Read dicom files
	binary_df = spark.read.format("binaryFile") \
		.option("pathGlobFilter", "*.dcm") \
		.option("recursiveFileLookup", "true") \
		.load(dicom_file_path)

	# Read parquet files
	dcm_df = spark.read.parquet(dcm_path)
	op_df = spark.read.parquet(op_path)

	# To improve read performance when you load data back, Databricks recommends 
	# turning off compression when you save data loaded from binary files:
	spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")

	# Create Delta tables
	create_delta_table(binary_df, binary_delta_path)
	create_delta_table(dcm_df, dcm_delta_path)
	create_delta_table(op_df, op_delta_path)


if __name__ == '__main__':
	spark_master = sys.argv[1]
	hdfs_ip = sys.argv[2]
	hdfs_path = sys.argv[3]
	delta_table_path = sys.argv[4]
	write_to_delta(spark_master,hdfs_ip,hdfs_path,delta_table_path)

