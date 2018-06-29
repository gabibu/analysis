
from sys import argv

def toCsv(file):

    hasMOreRows = True
    batch = 0
    with open(file, "r") as ins:

        while hasMOreRows:
            batchFile = 'batch_withIndex_{0}.csv'.format(batch)

            print('batchFile = {0}'.format(batchFile))

            with open(batchFile, 'w+') as csvfile:
                batch += 1
                print('start reading')
                csvfile.write('clicked,fn1,fn2,fn3,fn4,fn5,fn6,fn7,fn8,fn9,fn10,fn11,fn12,fn13,fc1,fc2,fc3,fc4,fc5,fc6,fc7,fc8,fc9,fc10,fc11,fc12,fc13,fc14,fc15,fc16,fc17,fc18,fc19,fc20,fc21,fc22,fc23,fc24,fc25,fc26,rowIndex\n')


                printCount = 0
                hasMOreRows = False

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

if __name__ == '__main__':

    if len(argv) < 2:
        raise Exception('missing aruments')

    toCsv(argv[1])