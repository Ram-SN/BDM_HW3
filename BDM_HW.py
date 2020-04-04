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

test = spark.sql('SELECT Product, "Date received" FROM df')

test.show()


# sc = SparkContext()

# file = spark.read.csv(input_file)

# file.head()