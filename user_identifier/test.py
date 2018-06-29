

# x1 = spark.sql('select count(*) from clicks where fn1 is null').collect()
# x2 = spark.sql('select count(*) from clicks where fn2 is null').collect()
# x3 = spark.sql('select count(*) from clicks where fn3 is null').collect()
# x4 = spark.sql('select count(*) from clicks where fn4 is null').collect()
# x5 = spark.sql('select count(*) from clicks where fn5 is null').collect()
# x6 = spark.sql('select count(*) from clicks where fn6 is null').collect()
# x7 = spark.sql('select count(*) from clicks where fn7 is null').collect()
# x8 = spark.sql('select count(*) from clicks where fn8 is null').collect()
# x9 = spark.sql('select count(*) from clicks where fn9 is null').collect()
# x10 = spark.sql('select count(*) from clicks where fn10 is null').collect()
# x11 = spark.sql('select count(*) from clicks where fn11 is null').collect()
# x12 = spark.sql('select count(*) from clicks where fn12 is null').collect()
# x13 = spark.sql('select count(*) from clicks where fn13 is null').collect()
#
# c1 = spark.sql('select count(*) from clicks where fc1 is null').collect()
# c2 = spark.sql('select count(*) from clicks where fc2 is null').collect()
# c3 = spark.sql('select count(*) from clicks where fc3 is null').collect()
# c4 = spark.sql('select count(*) from clicks where fc4 is null').collect()
# c5 = spark.sql('select count(*) from clicks where fc5 is null').collect()
# c6 = spark.sql('select count(*) from clicks where fc6 is null').collect()
# c7 = spark.sql('select count(*) from clicks where fc7 is null').collect()
# c8 = spark.sql('select count(*) from clicks where fc8 is null').collect()
# c9 = spark.sql('select count(*) from clicks where fc9 is null').collect()
# c10 = spark.sql('select count(*) from clicks where fc10 is null').collect()
# c11 = spark.sql('select count(*) from clicks where fc11 is null').collect()
# c12 = spark.sql('select count(*) from clicks where fc12 is null').collect()
# c13 = spark.sql('select count(*) from clicks where fc13 is null').collect()
# c14 = spark.sql('select count(*) from clicks where fc14 is null').collect()
# c15 = spark.sql('select count(*) from clicks where fc15 is null').collect()
# c16 = spark.sql('select count(*) from clicks where fc16 is null').collect()
# c17 = spark.sql('select count(*) from clicks where fc17 is null').collect()
# c18 = spark.sql('select count(*) from clicks where fc18 is null').collect()
# c19 = spark.sql('select count(*) from clicks where fc19 is null').collect()
# c20 = spark.sql('select count(*) from clicks where fc20 is null').collect()
# c21 = spark.sql('select count(*) from clicks where fc21 is null').collect()
# c22 = spark.sql('select count(*) from clicks where fc22 is null').collect()
# c23 = spark.sql('select count(*) from clicks where fc23 is null').collect()
# c24 = spark.sql('select count(*) from clicks where fc24 is null').collect()
# c25 = spark.sql('select count(*) from clicks where fc25 is null').collect()
# c26 = spark.sql('select count(*) from clicks where fc26 is null').collect()

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from scipy.stats import norm
import matplotlib.pyplot as plt
import pandas as pd




from matplotlib import pyplot

#x1 = pyplot.hist([1,2,3,4,5,6,7,8,9], bins=None)





#df = pd.read_csv('/home/gabib3b/mycode/git/optimalQTest/data/day_0_convertes.csv')

sc = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


spark = SQLContext(sc)


#data = spark.read.csv('/home/gabib3b/mycode/git/optimalQTest/data/5.csv', header=True)
data = spark.read.csv(['/home/gabib3b/mycode/git/optimalQTest/data/batch_0.csv',
                       '/home/gabib3b/mycode/git/optimalQTest/data/batch_1.csv'], header=True)


data.createTempView('clicks')

#res123 = spark.sql('select count(*) from (select distinct * from clicks)').collect()


arr = ['fn1', 'fn2','fn3','fn4','fn5','fn6','fn7', 'fn8','fn9', 'fc1', 'fc2', 'fc3', 'fc4', 'fc5', 'fc6','fc7',  'fc8',
       'fc9', 'fc10', 'fc11', 'fc12', 'fc13', 'fc14', 'fc15', 'fc16', 'fc17', 'fc18','fc19', 'fc20', 'fc21',
       'fc22', 'fc23', 'fc24', 'fc25', 'fc26']


fieldsCsv = ','.join(arr) #join(["'{0}'".format(x) for x in arr ] )
fieldsCsv2 = ','.join("'{0}'".format(x) for x in arr )

query = 'select case clicked when 1 then 1 else 0 end as clicked, case clicked when 1 then 0 else 1 end as nonclicked, {0} from clicks '.format(fieldsCsv)
d1 = spark.sql(query);

d1.createTempView('countClicks')
import numpy as np
for field in arr:

    # res1 = spark.sql('select percentile_approx(clicked, 0.5), percentile_approx(nonclicked, 0.5) from '
    #           '(select sum(clicked) as clicked, sum(nonclicked) as nonclicked, {0} as field from countClicks group by {0})'.format(field)).collect()

    res0 = spark.sql('select count(*) as numOfNulls from clicks where {0} is null'.format(field)).collect()

    nulls = res0[0]['numOfNulls']

    if nulls > 10:
        continue

    res1 = spark.sql('select  sum(clicked) as clicked, sum(nonclicked) as nonclicked, {0} as field from countClicks group by {0}'.format(
        field)).collect()



    unique = spark.sql('select distinct {0}  from clicks'.format(field)).count()

    clicks = []
    nonclicks = []
    adsPerUser = []
    pers = []
    zero = 0
    nonzero = 0

    for r in res1:
        c1 =r['clicked']
        clicks.append(c1)
        c2 = r['nonclicked']
        nonclicks.append(c2)
        adsPerUser.append(c1 + c2)

        pers.append(c1 /(c1 + c2))

        if c1 == 0:
            zero += 1

        else:
            nonzero += 1

    clicksMean = np.mean(clicks)
    clicksAvg = np.median(clicks)

    nonclicksmean = np.mean(nonclicks)
    nonclicksAvg = np.median(nonclicks)

    adsPerUsermean = np.mean(adsPerUser)

    adsPerUsermedian = np.median(adsPerUser)


    x1 = np.percentile(adsPerUser, 1)
    x2 = np.percentile(adsPerUser, 10)
    x3 = np.percentile(adsPerUser, 20)
    x4 = np.percentile(adsPerUser, 30)
    x5 = np.percentile(adsPerUser, 40)
    x6 = np.percentile(adsPerUser, 50)
    x7 = np.percentile(adsPerUser, 60)
    x8 = np.percentile(adsPerUser, 70)
    x9 = np.percentile(adsPerUser, 80)
    x10 = np.percentile(adsPerUser, 90)
    x11 = np.percentile(adsPerUser, 100)

    adsPerUserhist1, adsPerUserbin_edges1 = np.histogram(adsPerUser,
                                     bins=[100])

    r4 = np.mean(pers)
    percentaheOfUsersWithoutAnyClick = zero / (zero + nonzero)


    hist, bin_edges  = np.histogram(pers, bins=range(10))

    hist1, bin_edges1 = np.histogram(pers)

    hist1, bin_edges1 = np.histogram(pers)

    y1 = np.percentile(pers, 1)
    y2 = np.percentile(pers, 10)
    y3 = np.percentile(pers, 20)
    y4 = np.percentile(pers, 40)
    y5 = np.percentile(pers, 60)
    y6 = np.percentile(pers, 80)
    y7 = np.percentile(pers, 90)
    y8 = np.percentile(pers, 100)

    from matplotlib import pyplot

    pyplot.hist(pers, bins=None)

    pyplot.close()

    pyplot.hist(adsPerUser, bins=None)

    pyplot.close()

    pyplot.hist(adsPerUser, bins=[0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 40, 50, 90])

    pyplot.close()

    pyplot.hist(nonclicks, bins=[0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 40, 50, 90, 100, 130, 160, 200, 300, 400])

    pyplot.close()

    pyplot.hist(clicks, bins=[0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 30, 40, 50, 90, 100, 130, 160, 200, 300, 400])

    pyplot.close()

    mu = 0
    sigma = 1
    rng = range(-30, 30)

    # Generate normal distribution with given mean and standard deviation.
    dist = norm(mu, sigma)

    pyplot.subplot(311)  # Creates a 3 row, 1 column grid of plots, and renders the following chart in slot 1.
    pyplot.plot(adsPerUser, dist.pdf(adsPerUser), 'r', linewidth=2)
    pyplot.title('Probability density function of normal distribution')

    pyplot.close()

    print(res1)










#spark.sql('select clicked, count( fn8) as fn8, count( fn9) as fn9, count( fc2) as fc2, count( fc3) as fc3, count( fc4) as fc4, count( fc5) as fc5, count( fc6) as fc6, count( fc7) as fc7, count( fc8) as fc8, count( fc9) as fc9, count( fc12) as fc12,count( fc13) as fc13, count( fc15) as fc15, count( fc18) as fc18,count( fc19) as fc19, count( fc24) as fc24,count( fc25) as fc25, count( fc26) as fc26  from clicks group by clicked').collect()


#data = spark.sql('select * from clicks ').take(100)
#spark.sql('')

res1 = data.take(10)






print(pd)

print(1)
