import sys
import pyspark
from pyspark import SparkContext
import pyspark.sql.functions as func
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

input_file = sys.argv[1]

df = spark.read.csv(input_file, header=True, escape ='"', inferSchema = True, multiLine = True)

df.createOrReplaceTempView('df')

test = df.select('Date received', 'Product')

test.select(year('Date received')).show()
#test.show()