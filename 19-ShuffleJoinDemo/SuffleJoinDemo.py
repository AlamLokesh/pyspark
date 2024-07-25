from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    # Ensure we get 3 partitions after the shuffle
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    # Shuffle Join
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    join_df.collect()

    # Broadcast Join
    broadcast_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    broadcast_df.count()
    input("http://localhost:4040/jobs/ - http://localhost:4040/SQL/: press a key to stop...")

    spark.stop()
