import sys
import pyspark
from pyspark import SparkContext
import pyspark.sql.functions as func
import datetime
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

input_file = sys.argv[1]

df = spark.read.csv(input_file, header=True, escape ='"', inferSchema = True, multiLine = True)

df.createOrReplaceTempView('df')

test = df.select('Date received', 'Product', 'Company')

res1 = test.select(year('Date received').alias('year'), 'Product', 'Company').show()

#test.show()