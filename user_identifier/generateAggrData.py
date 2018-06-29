


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from scipy.stats import norm
import pandas as pd


import os






def prcess(index, file):

    sc = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


    spark = SQLContext(sc)


    #data = spark.read.csv('/home/gabib3b/mycode/git/optimalQTest/data/5.csv', header=True)
    data = spark.read.csv([file], header=True)


    data.createTempView('clicks')

    #res123 = spark.sql('select count(*) from (select distinct * from clicks)').collect()


    arr = ['fn1', 'fn2','fn3','fn4','fn5','fn6','fn7', 'fn8','fn9', 'fc1', 'fc2', 'fc3', 'fc4', 'fc5', 'fc6','fc7',  'fc8',
           'fc9', 'fc10', 'fc11', 'fc12', 'fc13', 'fc14', 'fc15', 'fc16', 'fc17', 'fc18','fc19', 'fc20', 'fc21',
           'fc22', 'fc23', 'fc24', 'fc25', 'fc26']


    fieldsCsv = ','.join(arr) #join(["'{0}'".format(x) for x in arr ] )

    query = 'select case clicked when 1 then 1 else 0 end as clicked, case clicked when 1 then 0 else 1 end as nonclicked, ' \
            '{0} from clicks '.format(fieldsCsv)

    d1 = spark.sql(query);

    d1.createTempView('countClicks')

    fieldToValues = {}
    for field in arr:
        fieldToValues[field] = {}
        # res1 = spark.sql('select percentile_approx(clicked, 0.5), percentile_approx(nonclicked, 0.5) from '
        #           '(select sum(clicked) as clicked, sum(nonclicked) as nonclicked, {0} as field from countClicks group by {0})'.format(field)).collect()

        numOfNullsData = spark.sql('select count(*) as numOfNulls from clicks where {0} is null'.format(field)).collect()

        numOfNulls = numOfNullsData[0]['numOfNulls']

        numOfUnique = spark.sql('select distinct {0}  from clicks'.format(field)).count()

        clickNoClickPerValue = spark.sql(
            'select  sum(clicked) as clicked, sum(nonclicked) as nonclicked, {0} as field from countClicks group by {0}'.format(
                field)).collect()

        valueToClicks = {}
        valueToNoClicks = {}
        for value in clickNoClickPerValue:
            clicked = value['clicked']
            nonclicked = value['nonclicked']
            fieldVal = value['field']
            valueToClicks[fieldVal] = clicked
            valueToNoClicks[fieldVal] =  nonclicked

        fieldToValues[field]['numOfNulls'] = numOfNulls
        fieldToValues[field]['numOfUnique'] = numOfUnique
        fieldToValues[field]['valueToClicks'] = valueToClicks
        fieldToValues[field]['valueToNoClicks'] = valueToNoClicks

    import json

    data = json.dumps(fieldToValues)

    with open('/home/gabib3b/mycode/git/optimalQTest/data/aggr/{0}.json'.format(index), 'w') as file:
        file.write(json.dumps(data, sort_keys=True))


dir="/home/gabib3b/mycode/git/optimalQTest/data"

files = [x[0]+"/"+f for x in os.walk(dir) for f in x[2] if f.startswith('batch_') and  f.endswith(".csv")]

for index,file in enumerate(files):
    prcess(index, file)



