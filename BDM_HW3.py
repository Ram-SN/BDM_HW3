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

res1 = test.select(year('Date received').alias('year'), 'Product', 'Company')

res2 = res1.groupBy('year', 'Product', 'Company').agg(func.count('Product').alias('Count_comp'))

#res3 = res2.groupBy('year','Product')

res4 = res2.groupBy('year','Product').agg(func.sum(res2.Count_comp).alias('Count_sum'),agg(func.max(res2.Count_comp).alias('Count_max'))

res4.show()

#test.show()