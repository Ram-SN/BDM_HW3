#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark import SparkContext


input_file = sys.argv[1]

output_file = sys.argv[2] ####name of the folder

file = spark.read.csv(input_file, header=True)

file.createOrReplaceTempView('file')

file.head()
