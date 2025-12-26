from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SimpleTest").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField('transaction_id', StringType()),
    StructField('timestamp', StringType()),
    StructField('customer_id', StringType()),
    StructField('product_id', StringType()),
    StructField('product_category', StringType()),
    StructField('product_name', StringType()),
    StructField('quantity', IntegerType()),
    StructField('unit_price', DoubleType()),
    StructField('total_amount', DoubleType()),
    StructField('payment_method', StringType()),
    StructField('country', StringType()),
    StructField('city', StringType())
])

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

transactions = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Show ALL transactions
query = transactions.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="3 seconds") \
    .start()

print("Showing ALL transactions...")
query.awaitTermination()