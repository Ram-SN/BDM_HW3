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

res2 = res1.groupBy('year', 'Product', 'Company').agg(func.count('Product').alias('Count_comp')).sort('year','Product')

res3 = res2.filter(res2.Count_comp >= 1).groupBy('year', 'Product').agg(func.count('Count_comp').alias('Count_comp_more')).sort('year','Product')

cond = [res2.year == res3.year, res2.Product == res3.Product]

res4 = res2.join(res3, ['year','Product'], 'left')

res4.show()

# res3 = res2.groupBy('year','Product').agg(func.sum('Count_comp').alias('Count_sum'))

# res4 = res2.groupBy('year','Product').agg(func.max('Count_comp').alias('Count_max'))

# cond = [res3.year == res4.year, res3.Product == res4.Product]

# res5 = res3.join(res4, cond, 'inner')

# res5 = res5.select(Product, year,)

# res2.show()
# test.show()