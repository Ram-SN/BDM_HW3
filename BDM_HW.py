#!/usr/bin/env python
# -*- coding: utf-8 -*-



import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession


def filterCol(records):
	for record in records:
        fields = record.split(',')
        yield(fields[1])








if __name__=='__main__':

	input_file = sys.argv[1]

	sc = SparkContext.getOrCreate()

	spark = SparkSession(sc)

	df = spark.read.csv(input_file, header=True)

	df.createOrReplaceTempView('df')

	df1 = df.withColumnRenamed('Date received', 'Date_received')

	df1.createOrReplaceTempView('df1')

	func = df1.mapPartions(filterCol)




 
#df1.show()

#test = spark.sql('SELECT Date_received, Product FROM df1')

#test.show()
