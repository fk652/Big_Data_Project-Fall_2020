from __future__ import print_function

import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":

    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def firstMap(line):
        line = next(reader([line]))

        if line[0] != 'United States':
            return []
        else:
            keyIndices = [0,1,2,4]
            dataLen = len(line)

            key = [line[i].strip() for i in keyIndices]
            key[3] = datetime.strptime(key[3], '%Y%m%d')
            value = [line[i].strip() for i in range(dataLen) if i not in keyIndices]

            floatIndices = [1, 3, 5, 7, 9, 11, 13, 15, 16, 18, 19, 20, 21, 23, 24, 25, 26, 27, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]
            for i in floatIndices:
                if value[i] != '':
                    value[i] = "%.2f" % float(value[i])

            intIndices = [2, 4, 6, 8, 10, 12, 14, 17, 22, 28, 30, 31]
            for i in intIndices:
                if value[i] != '':
                    value[i] = str(int(float(value[i])))
            
            return [(tuple(key), value)]

    cleanData = lines.flatMap(firstMap)
    cleanData = cleanData.sortBy(lambda x: (x[0][2], x[0][3]))

    def finalMap(line):
        key = list(line[0])
        value = line[1]
        key[3] = key[3].strftime('%Y-%m-%d')
        finalList = key[0:3] + value[0:1] + key[3:] + value[1:]
        joinedValue = ",".join(finalList)
        return joinedValue
    
    cleanData = cleanData.map(finalMap)
    cleanData.saveAsTextFile("oxford_clean.out")
    sc.stop()

