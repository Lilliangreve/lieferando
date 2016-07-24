# launch Spark (cmd prompt): C:\Progra~2\Spark\spark-1.6.2-bin-hadoop2.6\bin\pyspark
# Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame. 
# This conversion can be done using SQLContext.read.json on a JSON file. 
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
# import SQL function
from pyspark.sql.functions import *
from pyspark.sql.functions import col, count, sum
# read in data
Review = sqlContext.read.json("G:/takeaway.com/item_dedup.json.gz")
MetaData = sqlContext.read.json("G:/takeaway.com/metadata.json.gz")
Customer = sqlContext.read.json("G:/takeaway.com/user_dedup.json.gz")
# display column names
Review.first()
MetaData.first()
Customer.first()
# display count
Review.count()
MetaData.count()
Customer.count()
#convert to table
sqlContext.registerDataFrameAsTable(Review, "RW")
sqlContext.registerDataFrameAsTable(MetaData, "MD")
sqlContext.registerDataFrameAsTable(Customer, "CU")
# check duplicates (compare distinct counts to counts)
sqlContext.sql("SELECT count(distinct reviewerID) FROM RW").show()
sqlContext.sql("SELECT count(distinct MD.asin) FROM MD").show()
# take a look at some of the data ***Spark stopped responding ~5:30pm***
sqlContext.sql("SELECT salesRank FROM MD")
MetaData.select("brand").show()
#upload to postgres:
#Host: lilian-case.c9ensnzxd3la.eu-central-1.rds.amazonaws.com:5432/amazon
#user=root
#pass: zdHo2yd4JC5Th0
Review_2 = 		(sqlContext.load(
				source="jdbc", 
				url="jdbc:postgresql://lilian-case.c9ensnzxd3la.eu-central-1.rds.amazonaws.com:5432/amazon/postgres?user=root&password=zdHo2yd4JC5Th0", 
				dbtable="Review"))
				
MetaData_2 = 	(sqlContext.load(
				source="jdbc", 
				url="jdbc:postgresql://lilian-case.c9ensnzxd3la.eu-central-1.rds.amazonaws.com:5432/amazon/postgres?user=root&password=zdHo2yd4JC5Th0", 
				dbtable="MetaData"))
				
Customer_2 = 	(sqlContext.load(
				source="jdbc", 
				url="jdbc:postgresql://lilian-case.c9ensnzxd3la.eu-central-1.rds.amazonaws.com:5432/amazon/postgres?user=root&password=zdHo2yd4JC5Th0", 
				dbtable="Customer"))
