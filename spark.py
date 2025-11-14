from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("TestApp") \
    .master("local[*]") \
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .getOrCreate()

df = spark.createDataFrame([("Alice", 1)], ["Name", "ID"])
df.show()

spark.stop()
