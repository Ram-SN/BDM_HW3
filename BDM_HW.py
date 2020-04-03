#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark import SparkContext
import sys


input_file = sys.argv[1]

#output_file = sys.argv[2] ####name of the folder

sc = SparkContext()

file = sc.textFile(input_file)

# new_file = file.toDF()

print(file.take(5))

print("TESTING")
