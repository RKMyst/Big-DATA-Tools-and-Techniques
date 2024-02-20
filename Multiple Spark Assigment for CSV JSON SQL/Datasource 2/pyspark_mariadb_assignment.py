# https://kontext.tech/article/1061/pyspark-read-data-from-mariadb-database
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

appName = "PySpark Example"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()


server = "localhost"
port = 3306
database = "classicmodels"
jdbc_url = f"jdbc:mysql://{server}:{port}/{database}?permitMysqlScheme"


user = "root"
password = "test1234"
jdbc_driver = "org.mariadb.jdbc.Driver"

properties = {
    "user": user,
    "password": password,
    "driver": jdbc_driver
}

orders_df = spark.read.jdbc(jdbc_url, "(select * from orders) tab", properties=properties)
customers_df = spark.read.jdbc(jdbc_url, "(select * from customers) tab", properties=properties)
orderdetails_df = spark.read.jdbc(jdbc_url, "(select * from orderdetails) tab", properties=properties)
employees_df = spark.read.jdbc(jdbc_url, "(select * from employees) tab", properties=properties)
offices_df = spark.read.jdbc(jdbc_url, "(select * from offices) tab", properties=properties)

result_df = orders_df.join(customers_df, orders_df.customerNumber == customers_df.customerNumber) \
    .join(orderdetails_df, orders_df.orderNumber == orderdetails_df.orderNumber) \
    .join(employees_df, customers_df.salesRepEmployeeNumber == employees_df.employeeNumber) \
    .join(offices_df, employees_df.officeCode == offices_df.officeCode) \
    .groupBy(offices_df.city) \
    .agg(count(offices_df.city).alias("total_orders"), sum(orderdetails_df.quantityOrdered).alias("total_products_ordered"))
    
result_df.show()

# continents_df = spark.read.jdbc(jdbc_url, "(select * from continents) tab", properties=properties)
# countries_df = spark.read.jdbc(jdbc_url, "(select * from countries) tab", properties=properties)
# country_languages_df = spark.read.jdbc(jdbc_url, "(select * from country_languages) tab", properties=properties)
# country_stats_df = spark.read.jdbc(jdbc_url, "(select * from country_stats) tab", properties=properties)
# languages_df = spark.read.jdbc(jdbc_url, "(select * from languages) tab", properties=properties)
# regions_df = spark.read.jdbc(jdbc_url, "(select * from regions) tab", properties=properties)
# vips_df = spark.read.jdbc(jdbc_url, "(select * from vips) tab", properties=properties)
# guests_df = spark.read.jdbc(jdbc_url, "(select * from guests) tab", properties=properties)


# joined_df = (
#     countries_df
#     .join(regions_df, countries_df.region_id == regions_df.region_id, "left")
#     .join(continents_df, regions_df.continent_id == continents_df.continent_id, "left")
#     .join(country_languages_df, countries_df.country_id == country_languages_df.country_id, "left")
#     .join(languages_df, country_languages_df.language_id == languages_df.language_id, "left")
#     .join(country_stats_df, countries_df.country_id == country_stats_df.country_id, "left")
# )

# # Show the joined DataFrame
# joined_df.show()