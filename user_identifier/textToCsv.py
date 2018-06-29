import os, sys

# usage_string = 'usage: txt2csv.py {tr|te} input output'
#
# # if len(sys.argv) != 4:
# #     print(usage_string)
# #     exit(1)
#
# type = 'tr'
# src_path ='/home/gabib3b/mycode/git/optimalQTest/data/data.csv'
# dst_path  ='/home/gabib3b/mycode/git/optimalQTest/data/data1.csv'
#
# if type == 'tr':
#     header = 'Id,Label,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,C16,C17,C18,C19,C20,C21,C22,C23,C24,C25,C26'
#     idx = 10000000
# elif type == 'te':
#     header = 'Id,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,C16,C17,C18,C19,C20,C21,C22,C23,C24,C25,C26'
#     idx = 60000000
# else:
#     print(usage_string)
#     exit(1)
#
# with open(dst_path, 'w') as f:
#     f.write(header + '\r\n')
#     for line in open(src_path, 'r'):
#         line = str(idx) + ',' + line.replace('\t', ',')
#         f.write(line.replace('\n', '\r\n'))
#         idx += 1



hasMOreRows = True
batch = 0
#with open("/home/gabib3b/mycode/git/optimalQTest/data/test.csv", "r") as ins:
with open("/home/gabib3b/mycode/git/optimalQTest/data/day_0", "r") as ins:

    while hasMOreRows:
        batchFile = '/home/gabib3b/mycode/git/optimalQTest/data/batch_withIndex_{0}.csv'.format(batch)

        print('batchFile = {0}'.format(batchFile))

        with open(batchFile, 'w+') as csvfile:
            batch += 1
            print('start reading')
            csvfile.write('clicked,fn1,fn2,fn3,fn4,fn5,fn6,fn7,fn8,fn9,fn10,fn11,fn12,fn13,fc1,fc2,fc3,fc4,fc5,fc6,fc7,fc8,fc9,fc10,fc11,fc12,fc13,fc14,fc15,fc16,fc17,fc18,fc19,fc20,fc21,fc22,fc23,fc24,fc25,fc26,rowIndex\n')


            counter = 0
            printCount = 0
            hasMOreRows = False

            allFeatures = []
            lineCount = 0
            for line  in ins:
                print('processing line {0} batch {1} '.format(lineCount, batch-1))
                lineCount += 1

                line = line.strip()

                values = line.split('\t')
                lineFeatures = ''

                for val in values:
                    lineFeatures += val
                    lineFeatures += ','

                lineFeatures += str(lineCount)

                csvfile.write(lineFeatures)
                csvfile.write('\n')

                printCount += 1
                if printCount > 4000000:
                    hasMOreRows = True
                    break














# from pyspark import SparkContext
# sc = SparkContext("local", "First App")
# d1 = sc.textFile('/home/gabib3b/mycode/git/optimalQTest/data/temp.csv')
#
# x2 = d1.count()
#
# print(x2)