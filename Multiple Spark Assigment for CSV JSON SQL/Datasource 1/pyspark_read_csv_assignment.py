from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("HadoopCSVAnalysis").getOrCreate()


# Load CSV files into PySpark DataFrames
brands_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/brands_cleaned.csv", header=True)
categories_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/categories_cleaned.csv", header=True)
customers_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/customers_cleaned.csv", header=True)
orders_items_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/order_items_cleaned.csv", header=True)
orders_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/orders_cleaned.csv", header=True)
products_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/products_cleaned.csv", header=True)
staffs_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/staffs_cleaned.csv", header=True)
stores_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/stores_cleaned.csv", header=True)
stocks_df = spark.read.csv("hdfs://localhost:9000/user/input/bike_store/stocks_cleaned.csv", header=True)

print("Customers:")
customers_df.show()

print("Staff:")
staffs_df.show()

print("Orders:")
orders_df.show()

print("Order Items:")
orders_items_df.show()

# Joining the DataFrames and calculating total sales per customer
joined_ord_df = orders_df.join(customers_df, "customer_id", "inner")
joined_item_df = joined_ord_df.join(orders_items_df, "order_id", "inner")
total_sales_per_customer = joined_item_df.groupBy("city").agg(F.sum("total_price_discounted").alias("totals_sales"))

print("Total Sales per Customer:")
total_sales_per_customer.show()

# Convert PySpark DataFrame to Pandas DataFrame for further analysis
# total_sales_pd = total_sales_per_customer.toPandas()

# Stop the Spark session
spark.stop()

# employee_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/employees.csv", header=True)
# orders_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/orders.csv", header=True)
# product_categories_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/productcategories.csv", header=True)
# products_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/products.csv", header=True)
# product_subcategories_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/productsubcategories.csv", header=True)
# vendor_product_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/vendorproduct.csv", header=True)
# vendors_df = spark.read.csv("hdfs://localhost:9000/user/input/adventureworks/vendors.csv", header=True)

# Basic analysis example: Display the first few rows of each DataFrame
# print("Customers:")
# customers_df.show()

# print("Employee:")
# employee_df.show()

# print("Orders:")
# orders_df.show()

# ... Perform more basic analysis as needed ...

# Advanced analysis example: Joining and aggregating data
# Let's find the total sales amount per customer
# joined_df = orders_df.join(customers_df, "CustomerID", "inner")
# total_sales_per_customer = joined_df.groupBy("CustomerID", "FullName").agg({"TotalDue": "sum"})

# Show the result
# print("Total Sales per Customer:")
# total_sales_per_customer.show()

# Convert PySpark DataFrame to Pandas DataFrame for further analysis
# total_sales_pd = total_sales_per_customer.toPandas()

# Stop the Spark session
# spark.stop()
