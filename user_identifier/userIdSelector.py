from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import numpy as np
from matplotlib import pyplot
import scipy.stats as stats
import json

ALL_COLUMNS = ['fn1','fn2','fn3','fn4','fn5','fn6','fn7','fn8','fn9','fn10','fn11','fn12','fn13','fc1','fc2','fc3','fc4','fc5','fc6','fc7','fc8','fc9','fc10','fc11','fc12','fc13','fc14','fc15','fc16','fc17','fc18','fc19',
               'fc20','fc21','fc22','fc23','fc24','fc25','fc26']


def noNullColumns(spark, columns, features):

    inner = ''
    outer = ''
    for column in columns:
        oneZero = 'case   when {0} is null then 1 else 0  end as is_{0}_null,'.format(column)

        inner += oneZero
        outer += 'sum(is_{0}_null) as {0}_null_counter,'.format(column)




    query = 'select {0} from (select {1} from countClicks)'.format(outer[0:-1], inner[0:-1])
    isNullCollected = spark.sql(query).collect()[0]


    for column in columns:
        nullCounter = isNullCollected['{0}_null_counter'.format(column)]


        features[column]['numOfNUll'] = nullCounter + features[column]['numOfNUll']



def distinctValuesFilter(spark, columns, features):

    query = 'select '
    for column in columns:
        query +='COUNT(DISTINCT {0}) as {0},'.format(column)

    query = query[0:-1]
    query += ' from countClicks'
    columnDistinctValues = spark.sql(query).collect()[0]

    for column in columns:

        countDistinct = columnDistinctValues[column]
        features[column]['distinct'] = countDistinct



def plotAndClose(values):
    fit = stats.norm.pdf(values, np.mean(values), np.std(values))  # this is a fitting indeed

    pyplot.plot(values, fit, '-o')

    pyplot.hist(values, normed=True)  # use this to draw histogram of your data

    pyplot.show()
    pyplot.close()


def adsAndClicksPerUserFilter(spark, numOfRows, columns, features):

    for column in columns:
        rows = spark.sql('select  {0} as column_name, sum(clicked) as sum_click, sum(nonclicked) as sum_no_click, count(*) from countClicks group by {0}'.
                  format(column)).collect()

        addedQuery = 'select count(distinct(c1.{0})) as added from countClicks c1 left join  otherDataclicks other on other.{0} = c1.{0} where  other.{0} is null'
        sameQuery = 'select count(distinct(c1.{0})) as same from countClicks c1 inner join  otherDataclicks other on other.{0} = c1.{0}'

        added = spark.sql(addedQuery).collect()
        same = spark.sql(sameQuery).collect()



        clicks = []
        noClicks = []
        percentages = []
        impressionsPerUser = []
        usersWithClicks = 0
        usersWithoutClicks = 0
        nullValueData = {}
        clickOnlyUsersCounter = 0
        npClickOnlyUsersCounter = 0
        for row in rows:

            columnValue = row['column_name']


            clickSum = row['sum_click']

            if clickSum > 0:
                usersWithClicks += 1

            else:
                usersWithoutClicks += 1

            clicks.append(clickSum)
            noClickSum = row['sum_no_click']

            if clickSum > 0 and noClickSum == 0:
                clickOnlyUsersCounter += 1

            if clickSum == 0 and noClickSum > 0:
                npClickOnlyUsersCounter += 1

            noClicks.append(noClickSum)

            percentages.append(clickSum /(clickSum + noClickSum))
            impressionsPerUser.append(clickSum + noClickSum)

            if columnValue not in features[column]:
                features[column][columnValue] = {}

            if 'sumOfClicks' not in features[column][columnValue]:
                features[column][columnValue]['sumOfClicks'] = 0

            if 'sumOfNoClicks' not in features[column][columnValue]:
                features[column][columnValue]['sumOfNoClicks'] = 0

            features[column][columnValue]['sumOfClicks'] = features[column][columnValue]['sumOfClicks'] + clickSum
            features[column][columnValue]['sumOfNoClicks'] = features[column][columnValue]['sumOfNoClicks'] + noClickSum

            if columnValue == None:
                nullValueData['clickSum'] = clickSum
                nullValueData['noClickSum'] = noClickSum
                nullValueData['percentage'] = clickSum /(clickSum + noClickSum)

        numOfRows = features[column]['totalNumOfRows']
        numOfRowsPerFile = features[column]['numOfRowsPerFile']
        numOfNUll = features[column]['numOfNUll']
        #numOfDistinctValues = features[column]['numOfDistinctValues']
        distinctValues = features[column]['distinct']

        clicksMean = np.mean(clicks)
        impressionsPerUserMean = np.mean(impressionsPerUser)

        clicks.sort()
        impressionsPerUser.sort()
        percentages.sort()

        lastXClicks = clicks[-400:]
        lastXImpressionsPerUser= impressionsPerUser[-400:]
        lastXPercentages = percentages[-400:]

        perUsersWithClicks = usersWithClicks /(usersWithClicks + usersWithoutClicks)

        p1 = np.percentile(percentages, 1)
        p10 = np.percentile(percentages, 10)
        p20 = np.percentile(percentages, 20)
        p30 = np.percentile(percentages, 30)
        p40 = np.percentile(percentages, 40)
        p50 = np.percentile(percentages, 50)
        p60 = np.percentile(percentages, 60)
        p70 = np.percentile(percentages, 70)
        p80 = np.percentile(percentages, 80)
        p90 = np.percentile(percentages, 90)
        p95 = np.percentile(percentages, 95)
        p100 = np.percentile(percentages, 100)

        plotAndClose(percentages)




        c1 = np.percentile(clicks, 1)
        c10 = np.percentile(clicks, 10)
        c20 = np.percentile(clicks, 20)
        cp30 = np.percentile(clicks, 30)
        c40 = np.percentile(clicks, 40)
        c50 = np.percentile(clicks, 50)
        c60 = np.percentile(clicks, 60)
        c70 = np.percentile(clicks, 70)
        c80 = np.percentile(clicks, 80)
        c90 = np.percentile(clicks, 90)
        c95 = np.percentile(clicks, 95)
        c100 = np.percentile(clicks, 100)



        plotAndClose(clicks)

        im1 = np.percentile(impressionsPerUser, 1)
        im10 = np.percentile(impressionsPerUser, 10)
        im20 = np.percentile(impressionsPerUser, 20)
        im30 = np.percentile(impressionsPerUser, 30)
        im40 = np.percentile(impressionsPerUser, 40)
        im50 = np.percentile(impressionsPerUser, 50)
        im60 = np.percentile(impressionsPerUser, 60)
        im70 = np.percentile(impressionsPerUser, 70)
        im80 = np.percentile(impressionsPerUser, 80)
        im90 = np.percentile(impressionsPerUser, 90)
        im95 = np.percentile(impressionsPerUser, 95)
        im100 = np.percentile(impressionsPerUser, 100)

        plotAndClose(impressionsPerUser)

        print('done')


def clacForFile(features, file, otherFile):
    print('running on file {0}'.format(file))
    sc = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


    spark = SQLContext(sc)


    data = spark.read.csv(file , header=True).coalesce(100)
    #otherData = spark.read.csv(otherFile, header=True).coalesce(100)




    data.createTempView('clicks')
    #otherData.createTempView('otherDataclicks')
    #spark.cacheTable('clicks')

    fieldsCsv = ','.join(ALL_COLUMNS)

    query = 'select case clicked when 1 then 1 else 0 end as clicked, case clicked when 1 then 0 else 1 end as nonclicked, rowIndex, ' \
            '{0} from clicks '.format(fieldsCsv)

    d1 = spark.sql(query);

    d1.createTempView('countClicks')
    spark.cacheTable('countClicks')



    numOfRows = spark.sql('select count(*) as numOfRows from countClicks').take(1)[0]['numOfRows']

    print('running on file {0} and numOfRows{1}'.format(file, numOfRows))

    for column in ALL_COLUMNS:
        features[column]['totalNumOfRows'] = features[column]['totalNumOfRows'] + numOfRows
        features[column]['numOfRowsPerFile'].append(numOfRows)

    print('running on file {0} and noNullColumns'.format(file))

    noNullColumns(spark, ALL_COLUMNS, features)

    print('running on file {0} and distinctValuesFilter'.format(file))
    distinctValuesFilter(spark, ALL_COLUMNS, features)




    # spark.cacheTable('countClicks')

    print('running on file {0} and adsAndClicksPerUserFilter'.format(file))

    adsAndClicksPerUserFilter(spark, numOfRows, ALL_COLUMNS, features)

    print('running on file {0} and adsAndClicksPerUserFilter'.format(file))

    sc.stop()

    print('done')

if __name__ == '__main__':

    for fileIndex in range(1, 30):

        features = {}


        for column in ALL_COLUMNS:
            features[column] = {}
            features[column]['totalNumOfRows'] = 0
            features[column]['numOfRowsPerFile'] = []
            features[column]['numOfNUll'] = 0
            features[column]['numOfDistinctValues'] = 0

        print('running on index {0}'.format(fileIndex))

        file3 = '/home/gabib3b/Desktop/gabiData/batch_withIndex_0.csv'
        file4 = '/home/gabib3b/Desktop/gabiData/batch_withIndex_2.csv'

        clacForFile(features, file3, file4)

        if 1== 1:
            break

        print('clacForFile on index {0} completed'.format(fileIndex))

        data = json.dumps(features)

        print('dumps on index {0} completed'.format(fileIndex))

        with open('/home/gabib3b/mycode/git/optimalQTest/data/features/f_{0}.json'.format(fileIndex), 'w') as file:
            file.write(json.dumps(data, sort_keys=True))

        print('file.write on index {0} completed'.format(fileIndex))

