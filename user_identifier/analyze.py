
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import  StorageLevel
from scipy.stats import norm
import pandas as pd
import os
import logging
import numpy as np

# ALL_COLUMNS = ['fn1', 'fn2','fn3','fn4','fn5','fn6','fn7', 'fn8','fn9', 'fc1', 'fc2', 'fc3', 'fc4', 'fc5', 'fc6','fc7',  'fc8',
#            'fc9', 'fc10', 'fc11', 'fc12', 'fc13', 'fc14', 'fc15', 'fc16', 'fc17', 'fc18','fc19', 'fc20', 'fc21',
#            'fc22', 'fc23', 'fc24', 'fc25', 'fc26']

ALL_COLUMNS = ['fn1','fn2','fn3','fn4','fn5','fn6','fn7','fn8','fn9','fn10','fn11','fn12','fn13','fc1','fc2','fc3','fc4','fc5','fc6','fc7','fc8','fc9','fc10','fc11','fc12','fc13','fc14','fc15','fc16','fc17','fc18','fc19',
               'fc20','fc21','fc22','fc23','fc24','fc25','fc26']


def noNullColumns(spark, columns):

    #'select sum(isfn2null) from  (select case when fn1 is null then 1 else 0 end as isfn1null from clicks)
    nonNullColumns=  []
    inner = ''
    outer = ''
    for column in columns:
        oneZero = 'case   when {0} is null then 1 else 0  end as is_{0}_null,'.format(column)

        inner += oneZero
        outer += 'sum(is_{0}_null) as {0}_null_counter,'.format(column)


        #numOfNullsData = spark.sql('select count(*) as numOfNulls from clicks where {0} is null'.format(column)).collect()

        #if numOfNullsData[0]['numOfNulls'] == 0:
        #    nonNullColumns.append(columns)

    query = 'select {0} from (select {1} from clicks)'.format(outer[0:-1], inner[0:-1])
    isNullCollected = spark.sql(query).collect()[0]


    for column in columns:
        nullCounter = isNullCollected['{0}_null_counter'.format(column)]

        if nullCounter == 0:
            nonNullColumns.append(column)


    return nonNullColumns



def distinctValuesFilter(spark, numOfRows, columns):

    query = 'select '
    for column in columns:
        query +='COUNT(DISTINCT {0}) as {0},'.format(column)

    query = query[0:-1]
    query += ' from clicks'
    columnDistinctValues = spark.sql(query).collect()[0]

    validDistinct = []
    for column in columns:
        countDistinct = columnDistinctValues[column]
        avgNumOfRowsPerUser = numOfRows/countDistinct

        if avgNumOfRowsPerUser < 10000:
            validDistinct.append(column)

    return validDistinct



def adsAndClicksPerUserFilter(spark, numOfRows, columns):

    for column in columns:
        rows = spark.sql('select sum(clicked) as sum_click, sum(nonclicked) as sum_no_click, count(*) from countClicks group by {0}'.
                  format(column)).collect()

        clicks = []
        noClicks = []
        percentages = []
        adsPerUser = []
        for row in rows:
            clickSum = row['sum_click']
            noClickSum = row['sum_no_click']

            noClicks.append(noClickSum)
            clicks.append(clickSum)
            percentage = clickSum / (clickSum + noClickSum)
            percentages.append(percentage)
            adsPerUser.append(noClickSum + clickSum)


        y1 = np.percentile(percentages, 1)
        y2 = np.percentile(percentages, 50)
        y3 = np.percentile(percentages, 60)
        y4 = np.percentile(percentages, 70)
        y5 = np.percentile(percentages, 80)
        y6 = np.percentile(percentages, 85)
        y7 = np.percentile(percentages, 90)
        y8 = np.percentile(percentages, 95)
        y9 = np.percentile(percentages, 98)
        y10 = np.percentile(percentages, 99)
        y11 = np.percentile(percentages, 100)

        x1 = np.percentile(clicks, 1)
        x2 = np.percentile(clicks, 50)
        x3 = np.percentile(clicks, 60)
        x4 = np.percentile(clicks, 70)
        x5 = np.percentile(clicks, 80)
        y6 = np.percentile(clicks, 90)
        y7 = np.percentile(clicks, 95)
        y8 = np.percentile(clicks, 98)
        y9 = np.percentile(clicks, 99)
        y10 = np.percentile(clicks, 100)


        print('done')















sc = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


spark = SQLContext(sc)


data = spark.read.csv(['/home/gabib3b/mycode/git/optimalQTest/data/batch_withIndex_1.csv'
                          ,'/home/gabib3b/mycode/git/optimalQTest/data/batch_withIndex_2.csv'
                       ] , header=True)

#data = spark.read.csv(['/home/gabib3b/mycode/git/optimalQTest/data/tempwithindex.csv'], header=True)
#data.persist(StorageLevel.DISK_ONLY_2)

data.createTempView('clicks')


#noNUllColumns = noNullColumns(spark, ALL_COLUMNS)

noNUllColumns = ['fn8', 'fn9', 'fc2', 'fc3', 'fc4', 'fc5', 'fc6', 'fc7', 'fc8', 'fc9', 'fc12', 'fc13', 'fc15', 'fc18', 'fc19', 'fc24',
 'fc25', 'fc26']


numOfRows = spark.sql('select count(*) as numOfRows from clicks').take(1)[0]['numOfRows']


#validDistinctCountFilter = distinctValuesFilter(spark, numOfRows, noNUllColumns)

validDistinctCountFilter = ['fn8', 'fc2', 'fc3', 'fc4', 'fc5', 'fc7', 'fc12', 'fc15', 'fc24']

fieldsCsv = ','.join(ALL_COLUMNS)

query = 'select case clicked when 1 then 1 else 0 end as clicked, case clicked when 1 then 0 else 1 end as nonclicked, rowIndex, ' \
            '{0} from clicks '.format(fieldsCsv)

d1 = spark.sql(query);

d1.createTempView('countClicks')


validAdsAndClicksFilteredColumn = adsAndClicksPerUserFilter(spark, numOfRows, ALL_COLUMNS)


print(noNUllColumns)



