#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark import SparkContext
import sys

input_file = sys.argv[1]

sc = SparkContext()

file = spark.read.csv(input_file)

file.head()