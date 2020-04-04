#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pyspark import SparkContext
import sys


def sample_print(records):
    for record in records:
        fields = record.split(',')
        yield (fields)

if __name__=='__main__':

    input_file = sys.argv[1]

    sc = SparkContext()

    file = sc.textFile(input_file)

    output = file.mapPartitions(sample_print)

    print(output.collect())




# print("TESTING")
