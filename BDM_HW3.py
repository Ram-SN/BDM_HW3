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

res1 = res1.orderBy('Product','year')

res2 = res1.groupBy('year', 'Product', 'Company').agg(func.count('Product').alias('Count_comp'))

res3 = res2.groupBy('year', 'Product').agg(func.sum(count('Count_comp')).alias('Total Complaints'), func.countDistinct('Company').alias('Total Companies'), func.max(count('Count_comp')).alias('maximum'))

#res3 = res3.withColumn('Percentage',  func.round(func.col("Count_max") / func.col("Count_sum") * 100))

res3.show()
# cond = [res2.year == res3.year, res2.Product == res3.Product]

# res4 = res2.join(res3, ['year','Product'], 'inner')

# res4 = res4.sort('year','Product')

# res4 = res4.withColumn("Product",func.lower(func.col("Product")))

# res4 = res4.drop(res4.Company)

# res4 = res4.filter(res4.Count_comp >= 1)

# res4 = res4.drop(res4.Count_comp)

# #res4.show()

# res5 = res2.groupBy('year','Product').agg(func.sum('Count_comp').alias('Count_sum'))

# res6 = res2.groupBy('year','Product').agg(func.max('Count_comp').alias('Count_max'))

# res7 = res5.join(res6, ['year','Product'], 'inner')

# res7 = res7.filter(res7.Count_sum >=1)

# res7 = res7.withColumn('percentage', func.round(func.col("Count_max") / func.col("Count_sum") * 100))

# res7 = res7.withColumn("Product",func.lower(func.col("Product")))

# res7 = res7.drop(res7.Count_max)

# #res7.show()

# res8 = res7.join(res4,['year','Product'],'inner').sort('Product','year')

# res8.show()
