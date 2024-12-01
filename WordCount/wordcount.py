#!/usr/bin/python


#Entrypoint 2.x
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext

#------------------------------------------------

Rdd = sc.textFile("constitution.txt")

splitRDD = Rdd.flatMap(lambda line:line.split())

splitRDD_words = splitRDD.map(lambda word: (word,1)) 

resultRDD = splitRDD_words.reduceByKey(lambda x,y:x+y)

rdd_map = resultRDD.map(lambda tup:(tup[1],tup[0]))

rdd_sort = rdd_map.sortByKey(ascending = False)

print(rdd_sort.take(5))



