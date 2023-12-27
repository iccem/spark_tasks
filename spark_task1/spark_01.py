#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf
import re


def parseLine(line):
    try:
        temp0 = line.strip().split('\t')
        temp_2 = temp0[1].split(' ')
        t = []
        ii = len(temp_2)
        for i in range(0, ii - 1):
            temp = temp_2[i]
            temp_ = re.sub("^\W+|\W+$", "", temp)
            if temp_ == WORD:
                t_ = re.sub("^\W+|\W+$", "", temp_2[i + 1])
                t.append(t_)
        return t
    except ValueError as e:
        return ''


config = SparkConf().setAppName("spark_01_app").setMaster("yarn")
sc = SparkContext(conf=config)

dataset = '/data/wiki/en_articles_part'
WORD = 'narodnaya'

rdd = sc.textFile(dataset)

result = rdd.map(lambda x: x.strip().lower()) \
        .filter(lambda x: WORD in x) \
        .flatMap(lambda x: parseLine(x)) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

for i in result:
    w_ = i[0]
    c_ = i[1]
    print(WORD + '_' + w_+' '+str(c_))
