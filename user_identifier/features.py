import os, sys



hasMOreRows = True
batch = 0


columns = ['clicked','fn1','fn2','fn3','fn4','fn5','fn6','fn7','fn8','fn9','fn10','fn11','fn12','fn13','fc1','fc2','fc3','fc4','fc5','fc6','fc7','fc8','fc9','fc10','fc11','fc12','fc13','fc14','fc15','fc16','fc17','fc18','fc19','fc20','fc21','fc22','fc23','fc24', 'fc25','fc26']

fieldNumOfNUll = {}
numOfRows = 0
fieldValClicks = {}
fieldValNoClicks = {}
fieldValNUmOfRows = {}
fieldValRowsNumbers = {}

for column in columns:
    fieldNumOfNUll[column] = 0
    fieldValClicks[column] = {}
    fieldValNoClicks[column] = {}
    fieldValNUmOfRows[column] = {}
    fieldValRowsNumbers[column] = {}


def writeToFile(numOfRows, totaalClicks, totaalNoClicks, fieldNumOfNUll, fieldValClicks, fieldValNoClicks, fieldValNUmOfRows):
    data = {}
    data['numOfRows'] = numOfRows
    data['totalClicks'] = totaalClicks
    data['totalNoClicks'] = totaalNoClicks
    data['columns'] = {}
    data['columns']['fieldNumOfNUll'] = fieldNumOfNUll
    data['columns']['fieldValClicks'] = fieldValClicks

    data['columns']['fieldValNoClicks'] = fieldValNoClicks
    data['columns']['fieldValNUmOfRows'] = fieldValNUmOfRows

    import json
    with open('/home/gabib3b/mycode/git/optimalQTest/data/aggr/all_{0}.json'.format(numOfRows), 'w') as file:
        file.write(json.dumps(data, sort_keys=True))


#with open("/home/gabib3b/mycode/git/optimalQTest/data/test.csv", "r") as ins:
totaalClicks = 0
totaalNoClicks = 0
stop = 4000000
with open("/home/gabib3b/mycode/git/optimalQTest/data/day_0", "r") as ins:

    for line  in ins:
        numOfRows += 1
        print('row {0}'.format(numOfRows))

        line = line.strip()

        values = line.split('\t')
        lineFeatures = ''
        isClicked = values[0] == '1'

        if isClicked:
            totaalClicks += 1
        else:
            totaalNoClicks += 1

        for index, val in enumerate(values[1:]):
            column = columns[index + 1]


            if val == '':
                fieldNumOfNUll[column] = fieldNumOfNUll[column] + 1

            else:

                if val not in fieldValRowsNumbers[column]:
                        fieldValRowsNumbers[column][val] = []

                fieldValRowsNumbers[column][val].append(numOfRows)

                if val not in fieldValClicks[column]:
                    fieldValClicks[column][val] = 0

                if val not in fieldValNoClicks[column]:
                    fieldValNoClicks[column][val] = 0

                if val not in fieldValNUmOfRows[column]:
                    fieldValNUmOfRows[column][val] = 0

                fieldValNUmOfRows[column][val] = fieldValNUmOfRows[column][val] + 1

                if isClicked:
                    fieldValClicks[column][val] = fieldValClicks[column][val] + 1
                else:
                    fieldValNoClicks[column][val] = fieldValNoClicks[column][val] + 1

        if numOfRows > stop:
            print('write to file')
            writeToFile(numOfRows, totaalClicks,
                        totaalNoClicks, fieldNumOfNUll, fieldValClicks, fieldValNoClicks, fieldValNUmOfRows)

            print('write to file done')
            next = stop * 2
            print('write to file done current {0} next {1}'.format(stop, next))

            stop = next






writeToFile(numOfRows, totaalClicks, totaalNoClicks, fieldNumOfNUll, fieldValClicks, fieldValNoClicks,
                            fieldValNUmOfRows)








