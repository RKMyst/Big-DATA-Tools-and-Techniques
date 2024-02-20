import findspark
findspark.init()
import pyspark
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("FirstRun")\
.config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")\
.getOrCreate()

server_name = "jdbc:sqlserver://alinizarserver.database.windows.net"
database_name = "myfirstdatabase"
url = server_name + ";" + "databaseName=" + database_name + ";"
table_name = "myfirsttable"
jdbcDF = spark.read\
.format("com.microsoft.sqlserver.jdbc.spark")\
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
.option("url", url)\
.option("dbtable", table_name)\
.option("user", "azureuser")\
.option("password", "Ilove85workWonder69").load()



jdbcDF.show()