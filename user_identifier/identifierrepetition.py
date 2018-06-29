
import logging
import pandas
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
from bloomFilter import BloomFilter


def userIds(file1, column):
    clicksDataFrame = pandas.read_csv(file1, header='infer')

    clicksDataFrame.rename(columns={column: 'userId'}, inplace=True)

    clickedUsersIds = clicksDataFrame[clicksDataFrame.userId.notnull()][
        'userId'].unique().tolist()

    return clickedUsersIds


def sampleData(file1, file2, column):


        filter = BloomFilter(13419082, 23)

        firstUsersIds1 = userIds(file1, column)

        for user in firstUsersIds1:
            filter.add(str(user))


        firstUsersIds2 = userIds(file2, 'fc20')

        same = 0
        diff = 0
        for user in firstUsersIds2:
            if filter.contains(str(user)):
                same += 1
            else:
                diff += 1

        return same, diff


if __name__ == '__main__':
        same1, diff1 = sampleData('batch_withIndex_0.csv',
                   'batch_withIndex_1.csv', 'fc1')

        print('{0} {1}'.format(same1, diff1))
        same2, diff2 = sampleData('batch_withIndex_0.csv',
                                'batch_withIndex_2.csv', 'fc1')

        print('{0} {1}'.format(same2, diff2))
        same3, diff3 = sampleData('batch_withIndex_1.csv',
                                'batch_withIndex_2.csv', 'fc1')

        print('{0} {1}'.format(same3, diff3))
        same11, diff11 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_1.csv', 'fc10')

        print('{0} {1}'.format(same11, diff11))
        same12, diff12 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_2.csv', 'fc10')

        print('{0} {1}'.format(same12, diff12))

        same13, diff13 = sampleData('batch_withIndex_1.csv',
                                  'batch_withIndex_2.csv', 'fc10')

        print('{0} {1}'.format(same13, diff13))
        same14, diff14 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_1.csv', 'fc20')

        print('{0} {1}'.format(same14, diff14))

        same24, diff24 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_2.csv', 'fc20')

        print('{0} {1}'.format(same24, diff24))
        same34, diff34 = sampleData('batch_withIndex_1.csv',
                                  'batch_withIndex_2.csv', 'fc20')

        print('{0} {1}'.format(same34, diff34))
        same15, diff15 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_1.csv', 'fc21')

        print('{0} {1}'.format(same15, diff15))
        same25, diff25 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_2.csv', 'fc21')

        print('{0} {1}'.format(same25, diff25))
        same35, diff35 = sampleData('batch_withIndex_1.csv',
                                  'batch_withIndex_2.csv', 'fc21')

        print('{0} {1}'.format(same35, diff35))
        same16, diff16 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_1.csv', 'fc22')

        print('{0} {1}'.format(same16, diff16))
        same26, diff26 = sampleData('batch_withIndex_0.csv',
                                  'batch_withIndex_2.csv', 'fc22')

        print('{0} {1}'.format(same26, diff26))
        same36, diff36 = sampleData('batch_withIndex_1.csv',
                                  'batch_withIndex_2.csv', 'fc22')

        print('{0} {1}'.format(same36, diff36))
        print('done')


