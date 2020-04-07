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

output_file = sys.argv[2]

df = spark.read.csv(input_file, header=True, escape ='"', inferSchema = True, multiLine = True)

df.createOrReplaceTempView('df')

test = df.select('Date received', 'Product', 'Company')

res1 = test.select(year('Date received').alias('year'), 'Product', 'Company')

res1 = res1.orderBy('Product','year')

res2 = res1.groupBy('year', 'Product', 'Company').agg(func.count('Product').alias('Count_comp'))

res3 = res2.groupBy('year', 'Product').agg(func.sum('Count_comp').alias('Total_Complaints'), func.countDistinct('Company').alias('Total_Companies'), func.max('Count_comp').alias('maximum'))

res3 = res3.filter(res3.Total_Complaints>= 1)

res4 = res3.withColumn('Percentage', func.round(func.col('maximum') / func.col('Total_Complaints') * 100))

res4 = res4.drop(res4.maximum).sort('year', 'Product')

res4 = res4.select(func.lower('Product'), 'year', 'Total_Complaints', 'Total_Companies', 'Percentage')

res4.show()

