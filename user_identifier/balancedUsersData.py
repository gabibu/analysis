
from bloomFilter import BloomFilter

class BalanedData:

    def __init__(self, filterSize, hashCount, clickedUsers):
        self.allData =[]
        self.clickedCounter = len(clickedUsers)
        self.noClickedCounter = 0
        self.collectedDataUsersFilter = BloomFilter(filterSize, hashCount)
        self.__addUsers(clickedUsers)


    def __addUsers(self, clickedUsersIds):
        for userId in clickedUsersIds:
            self.__addUser(userId)

    def __addUser(self, userId):
        self.collectedDataUsersFilter.add(userId)

    def addUserRow(self, userId, row):

        isCollected = self.collectedDataUsersFilter.contains(userId)

        if isCollected:
            self.allData.append(row)
        elif self.clickedCounter > self.noClickedCounter:
            self.__addUser(userId)
            self.noClickedCounter += 1
            self.allData.append(row)







