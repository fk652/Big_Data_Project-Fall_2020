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
            keyIndices = [2,4]
            dataLen = len(line)

            key = [line[i].strip() for i in keyIndices]
            key[1] = datetime.strptime(key[1], '%Y%m%d')
            value = [line[i].strip() for i in range(dataLen) if i not in keyIndices]

            floatIndices = [3, 5, 7, 9, 11, 13, 15, 17, 18, 20, 21, 22, 23, 25, 26, 27, 28, 29, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43]
            for i in floatIndices:
                if value[i] != '':
                    value[i] = "%.2f" % float(value[i])

            intIndices = [4, 6, 8, 10, 12, 14, 16, 19, 24, 30, 32, 33]
            for i in intIndices:
                if value[i] != '':
                    value[i] = str(int(float(value[i])))
            
            return [(tuple(key), value)]

    cleanData = lines.flatMap(firstMap)
    cleanData = cleanData.sortByKey(ascending=True)

    def finalMap(line):
        key = list(line[0])
        value = line[1]
        key[1] = key[1].strftime('%Y-%m-%d')
        finalList = value[:2] + [key[0]] + [value[2]] + [key[1]] + value[3:]
        joinedValue = ",".join(finalList)
        return joinedValue
    
    cleanData = cleanData.map(finalMap)
    cleanData.saveAsTextFile("oxford_clean.out")
    sc.stop()
