from pyspark import SparkContext
from operator import add


#Question 3.1 How much did each seller earn? Develop the Spark RDD
# (Spark RDD means using only the transformations and actions, no DataFrame/SQL) version to solve the query.


#Creates sparkContext
sc=SparkContext(appName="seller_earn_RDD_transformation_actions")
#Load purchase file using sc context
purchaseddata=sc.textFile("Pyspark/purchase")

# get each record from purchase file
data_splitbylines=purchaseddata.flatMap(lambda i:i.split("\n"))

# get each column from a row in the purchases data
data_splitbytab=data_splitbylines.map(lambda  j:j.split("\t"))

#seller and price is passed as key value pair to reduce function with add operation
# compute the income of each seller
result_seller_earned=data_splitbytab.map(lambda z:(z[3],int(z[4]))).reduceByKey(add)

print("======" * 4)
#prints sales of each seller
print("Each seller earns {}".format(result_seller_earned.collect()))
print("======" * 4)

