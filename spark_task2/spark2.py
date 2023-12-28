#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession.builder.appName('spark_02_app').master('yarn').getOrCreate()

    dataset = "/data/twitter/twitter_sample.txt"

    START = 12
    END = 34

    result_schema = StructType([
        StructField("START", IntegerType(), False),
        StructField("END", IntegerType(), False)
    ])

    edges_schema = StructType(fields=[
        StructField("START", IntegerType()),
        StructField("END", IntegerType())
    ])

    df = spark.read.format("csv").schema(edges_schema).option("sep", "\t").load(dataset)
    df2 = df.distinct()
    edges = df2.coalesce(1).cache()

    path = spark.createDataFrame([(START, END)], result_schema)

    current_step = 1

    temp_START = edges.where('START == 34')

    path = (
        temp_START.alias('t')
        .select(
            col('t.START').alias('START{}'.format(current_step)),
            col('t.END').alias('END{}'.format(current_step)))
    )

    t = temp_START

    while True:
        current_temp = (
            t.alias("r1").join(edges.alias("r2"), col("r1.END") == col("r2.START"))
            .select(
                col("r2.START"),
                col("r2.END")).where('r2.END != r1.START')
        )

        flag = current_temp.where('START == 12 or END == 12')
        cnt = flag.count()

        temp_for_path = (
            current_temp.alias('t')
            .select(
                col('t.START').alias('START{}'.format(current_step + 1)),
                col('t.END').alias('END{}'.format(current_step + 1))))

        tt = (
            path.alias('path')
            .join(
                temp_for_path.alias('t'),
                col('path.END{}'.format(current_step)) == col('START{}'.format(current_step + 1))))

        path = tt.distinct().cache()

        if cnt > 0:
            path = path.coalesce(1).cache()
            break
        else:
            current_step += 1
            t = current_temp


    edges.unpersist()
    c_path = path.where('END{} == 12'.format(current_step + 1)).coalesce(1).cache()
    paths = c_path.collect()
    dd = paths[0].asDict()
    total_path = list()

    for i in range(1, current_step+2):
        n = 'START'+str(i)
        total_path.appEND(dd[n])
    total_path.appEND(START)


    print_path = total_path[::-1]
    print(','.join(str(i) for i in print_path))
