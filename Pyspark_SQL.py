#Question 3.2 Find the names of the books that Amazon gives the lowest price among all sellers,
# excluding the cases that other sellers also give the lowest price. Develop the DataFrame (using Spark SQL) version to solve this query.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from colorama import Fore,Style

'creata a spark session to use SQL query procession'
spark=SparkSession.builder.appName("Pysparl_SQLDemo").master("local").getOrCreate()
'Load the file and store in list format'
purchases=spark.sparkContext.textFile("Pyspark/purchase").flatMap(lambda i:i.split("\n")).map(lambda  j:j.split("\t"))
#print(purchases.collect())
'Define Schema with column names and its type for loaded data'
p_schema=StructType([
    StructField("year",StringType()),
    StructField("cid",StringType()),
    StructField("isbn",StringType()),
    StructField("seller",StringType()),
    StructField("price",StringType())])
'load books file '
book=spark.sparkContext.textFile("Pyspark/book").flatMap(lambda i:i.split("\n")).map(lambda  j:j.split("\t"))
'Schema for book file'
b_schema=StructType([
    StructField("isbn",StringType()),
    StructField("name",StringType())])
'Dataframe created for purchase and book with P_schema and b_schema structure'
purchase_df=spark.createDataFrame(purchases,p_schema)
book_df=spark.createDataFrame(book,b_schema)
#purchase_df.printSchema()

#print(purchase_df.collect())
'Creates temporary view for purchase_df and book_df dataframe'
purchase_df.createOrReplaceTempView("purchase_table")
book_df.createOrReplaceTempView("book_table")
#,purchase_table.seller,purchase_table.Price,newT.Min_price
'SQL query can be run using SQL statment provided spark for fetching details of book name sold by amazon for lowest price'
result=spark.sql("select name from book_table where isbn IN (select purchase_table.isbn from  purchase_table INNER JOIN (select MIN(CAST(price AS int)) as Min_price from purchase_table group by isbn) resultT on purchase_table.price=resultT.Min_price and purchase_table.seller='Amazon')")

print(Fore.YELLOW+"The names of the books that Amazon gives the lowest price among all sellers\n")
'prints result to console'
result.show()
print(Style.RESET_ALL)
