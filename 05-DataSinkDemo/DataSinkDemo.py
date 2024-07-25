from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    # Writes only 1 file because all the records are in the partition 0 (even though there are 2 partitions)
    # flightTimeParquetDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()

    # This will take some time to complete
    (flightTimeParquetDF.write
     .format("json")
     .mode("overwrite")
     .option("path", "dataSink/json/")
     .partitionBy("OP_CARRIER", "ORIGIN")
     .option("maxRecordsPerFile", 10000)  # Use this to control the number of records per file and file size
     .save())

    spark.stop()
