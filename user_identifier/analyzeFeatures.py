from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import json

# sc = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
#
#
# spark = SQLContext(sc)

#data1 = spark.read.json('/home/gabib3b/mycode/git/optimalQTest/data/features/f_1.json')



#json_data=open('/home/gabib3b/mycode/git/optimalQTest/data/features/f_0.json').read()
with open('/home/gabib3b/mycode/git/optimalQTest/data/features/f_0.json') as json_file:
    data = json.load(json_file)


#dataD = json.loads(json_data)


for column, features in data.items():
    print(column)


print(11)



