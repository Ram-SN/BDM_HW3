#!/usr/bin/env python
# -*- coding: utf-8 -*-



import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

input_file = sys.argv[1]

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.csv(input_file, header=True)

df.createOrReplaceTempView('df')

# df.show()

# df.printSchema()

df1 = df.withColumnRenamed('Date received', 'Date_received')

df1.createOrReplaceTempView('df1')

df1.printSchema()

# test = spark.sql('SELECT EXTRACT(YEAR FROM Date_received), Product FROM df1')

# test.show()
