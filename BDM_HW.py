#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark import SparkContext
import sys

input_file = sys.argv[1]

sc = SparkContext()

file = sc.textFile(input_file)

file.collect()