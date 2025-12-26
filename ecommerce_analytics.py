from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window

spark = SparkSession.builder\
            .appName("EcommerceAnalytics")\
                .master("local[*]")\
                    .config("spark.sql.shuffle.partitions", "2")\
                        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created successfully")

# Define Schema
transaction_schema = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('timestamp', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('product_id', StringType(), True),
    StructField('product_category', StringType(), True),
    StructField('product_name', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('unit_price', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_method', StringType(), True),
    StructField('country', StringType(), True),
    StructField('city', StringType(), True)
])

# Connect to Socket and Read Stream
raw_stream = spark.readStream\
                .format("socket")\
                    .option("host", "localhost")\
                        .option("port", 9999)\
                            .load()

print("Connected to socket at localhost:9999")

# Parse JSON Data
transactions = raw_stream.select(
    from_json(col("value"), transaction_schema).alias("data")
).select("data.*")

transactions = transactions.withColumn(
    "fetched_at",
    current_timestamp()
)

print("CONFIGURING ANALYTICS STREAMS")

# ANALYTICS STREAM 1 - Real-time Sales Dashboard
sales_dashboard = transactions.agg(
    count("*").alias("total_transactions"),
    sum("total_amount").alias("total_revenue"),
    round(avg("total_amount"), 2).alias("avg_transaction_value"),
    sum("quantity").alias("total_items_sold")
)

sales_dashboard = sales_dashboard.withColumn(
    "total_revenue_formatted",
    concat(lit("$"), format_number(col("total_revenue"), 2))
)

query1 = sales_dashboard.writeStream\
            .outputMode("complete")\
                .format("console")\
                    .option("truncate", False)\
                        .queryName("SalesDashboard")\
                            .trigger(processingTime="10 seconds")\
                                .start()

# ANALYTICS STREAM 2 - Sales by Category
category_sales = transactions.groupBy("product_category").agg(
    count("*").alias("num_transactions"),
    sum("total_amount").alias("category_revenue"),
    sum("quantity").alias("items_sold"),
    round(avg("total_amount"), 2).alias("avg_order_value")
).orderBy(desc("category_revenue"))

query2 = category_sales.writeStream\
            .outputMode("complete")\
                .format("console")\
                    .option("truncate", False)\
                        .queryName("CategorySales")\
                            .trigger(processingTime="10 seconds")\
                                .start()

# ANALYTICS STREAM 3 - Geographic Analysis
geographic_sales = transactions.groupBy("country", "city").agg(
    count("*").alias("transactions"),
    round(sum("total_amount"), 2).alias("revenue"),
    round(avg("total_amount"), 2).alias("avg_order")
).orderBy(desc("revenue"))

query3 = geographic_sales.writeStream\
            .outputMode("complete")\
                .format("console")\
                    .option("truncate", False)\
                        .queryName("GeographicalSales")\
                            .trigger(processingTime="15 seconds")\
                                .start()

# ANALYTICS STREAM 4 - Top Products (Windowed)
transactions_with_time = transactions.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

top_products = transactions_with_time \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "2 minutes", "30 seconds"),
        col("product_name"),
        col("product_category")
    ).agg(
        count("*").alias("purchase_count"),
        sum("quantity").alias("units_sold"),
        round(sum("total_amount"), 2).alias("revenue")
    )

query4 = top_products.writeStream\
            .outputMode("update")\
                .format("console")\
                    .option("truncate", False)\
                        .queryName("TopProducts")\
                            .trigger(processingTime="30 seconds")\
                                .start()

# ANALYTICS STREAM 5 - Fraud Detection
high_value_txns = transactions.filter(col("total_amount") > 300)\
    .select(
        col("transaction_id"),
        col("customer_id"),
        col("total_amount"),
        col("product_name"),
        col("country"),
        lit("High Value Transaction").alias("alert_type")
    )

query5 = high_value_txns.writeStream\
            .outputMode("append")\
                .format("console")\
                    .option("truncate", False)\
                        .queryName("FraudAlerts")\
                            .trigger(processingTime="5 seconds")\
                                .start()

# ANALYTICS STREAM 6 - Payment Method Analysis
payment_analysis = transactions.groupBy("payment_method").agg(
    count("*").alias("num_transactions"),
    round(sum("total_amount"), 2).alias("total_revenue"),
    round(avg("total_amount"), 2).alias("avg_transaction")
).orderBy(desc("num_transactions")) 

query6 = payment_analysis.writeStream\
            .outputMode("complete")\
                .format("console")\
                    .option('truncate', False)\
                        .queryName("PaymentAnalysis")\
                            .trigger(processingTime="15 seconds")\
                                .start()

print("\n" + "="*60)
print("ALL ANALYTICS STREAMS STARTED SUCCESSFULLY")
print("="*60)
print("\nActive Queries:")
print("  1. Sales Dashboard (every 10s)")
print("  2. Category Sales (every 10s)")
print("  3. Geographic Analysis (every 15s)")
print("  4. Top Products - Windowed (every 30s)")
print("  5. Fraud Alerts (every 5s)")
print("  6. Payment Analysis (every 15s)")
print("\n" + "="*60)
print("Monitoring started. Press Ctrl+C to stop.")
print("="*60 + "\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\n" + "="*60)
    print("STOPPING ALL STREAMS")
    print("="*60)

    for query in spark.streams.active:
        print(f"Stopping {query.name}...")
        query.stop()

    print("\n All streams stopped successfully")
    print(" Spark session closed")
    print("="*60 + "\n")
    
    spark.stop()