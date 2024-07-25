from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .master("local[3]")
             .appName("SparkSQLTableDemo")
             .enableHiveSupport()  # Needed to allow the connectivity to a persistent Hive metastore
             .getOrCreate())

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")  # If not set, it will default to the default database

    (flightTimeParquetDF.write
     # .format("csv")  # Uncomment if you would like to inspect the records, or use parquet plugin for PyCharm
     .mode("overwrite")
     # .partitionBy("ORIGIN", "OP_CARRIER")  # You do not want to partition on a column with too many unique values
     .bucketBy(5, "OP_CARRIER", "ORIGIN")  # Bucketing is a way to distribute data across a fixed number of files
     .sortBy("OP_CARRIER", "ORIGIN")  # Companion for bucketBy, it allows the files to be ready by certain operations
     .saveAsTable("flight_data_tbl"))  # Alternatively you can use saveAsTable("AIRLINE_DB.flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))

    spark.stop()