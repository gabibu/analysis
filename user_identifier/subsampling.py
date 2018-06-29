
import logging
from balancedUsersData import BalanedData
import pandas
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
USER_ID_COLUMN= 'fc20'
import sys


def sampleData(file):

        logger.info('reading file {0}'.format(file))

        clicksDataFrame = pandas.read_csv(file, header='infer')


        clicksDataFrame.rename(columns={USER_ID_COLUMN: 'userId'}, inplace=True)


        clickedUsersIds = clicksDataFrame[clicksDataFrame.userId.notnull() & (clicksDataFrame.clicked == 1)]['userId'].unique().tolist()


        balancedData = BalanedData(13419082, 23, clickedUsersIds)

        with open(file, "r") as lines:

                header = next(lines)
                userIdIndex = header.split(',').index(USER_ID_COLUMN)

                for line in lines:
                        userId = line.split(',')[userIdIndex]
                        balancedData.addUserRow(userId, line)



        logger.info('completed')

if __name__ == '__main__':

        if len(sys.argv) < 2:
                raise Exception('missing arguments')

        file = sys.argv[1]
        sampleData(file)







