from pyspark import SparkContext
import pyspark.sql.functions as func
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

input_file = sys.argv[1]

df = spark.read.csv(input_file, header=True, escape ='"', inferSchema = True, multiline = True)

df.createOrReplaceTempView('df')

test = spark.sql('SELECT EXTRACT(YEAR FROM Date received), Product FROM df')

test.show()