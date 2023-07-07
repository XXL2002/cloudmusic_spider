#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Filename: rdd_1.py

from pyspark import SparkConf, SparkContext

#构建Spark接口对象 SparkConf
#设置运行模式
#调用算子处理数据
#关闭进程


if __name__ == '__main__':
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("rdd-1")
    # conf = SparkConf().setMaster("local[*]").setAppName("rdd-1")
    sc = SparkContext(conf=conf)

    def my_map():
        data = [1,2,3,4,5]
        rdd1 = sc.parallelize(data)     # 将本地集合转换为分布式数据集RDD
        #rdd2 = rdd1.map(lambda x:x*2)
        rdd2 = rdd1.map(lambda x:x*2)
        #rdd3 = rdd2.mapPartitions(lambda x:x*2)
        print(rdd2.collect())   # 将RDD中的所有元素收集到驱动程序中，并以一个本地集合的形式返回

    def my_map2():
        a = sc.parallelize(["dog", "lion", "cat", "tiger"])
        b = a.map(lambda x:(x,1))  #(dog,1)
        print(b.collect())

    def my_filter():
        data = [1,2,3,4,5]
        rdd1 = sc.parallelize(data)
        mapRdd = rdd1.map(lambda x:x*2).filter(lambda a:a>5)
        print(mapRdd.collect())

        # print(sc.parallelize(data).map(lambda x:x*2).filter(lambda x:x>5).collect())

    def my_flatMap():
        data = ["hello spark", "hello world", "hello spark"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).collect())   # 先执行map的操作，再合并为一个对象

    def my_groupByKey():
        data = ["hello spark", "hello world", "hello spark"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x, 1)).groupByKey().collect())
        print(rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x, 1)).groupByKey().map(lambda x:{x[0]:list(x[1])}).collect())

    def my_reduceByKey():
        data = ["hello spark", "hello world", "hello spark"]
        rdd = sc.parallelize(data)
        mapRdd = rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
        reduceRdd = mapRdd.reduceByKey(lambda a,b:a+b).collect()
        print(reduceRdd)

        #print(sc.parallelize(data).flatMap(lambda line:line.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).collect())

    def my_union():
        a = sc.parallelize([1, 2, 3, 4, 5])
        b = sc.parallelize([5, 4, 6, 7, 8])
        print(a.union(b).collect())
        print(a.zip(b).collect())

    def my_distinct():
        a = sc.parallelize([1, 2, 3, 4, 5])
        b = sc.parallelize([5, 4, 6, 7])
        print(a.union(b).distinct().collect())

    def my_intersection():
        a = sc.parallelize([1, 2, 3, 4, 5])
        b = sc.parallelize([5, 4, 6, 7])
        print(a.intersection(b).collect())

    def my_join():
        #只用于二元元祖-------kv键值对
        #关联条件 指定为key
        a = sc.parallelize([("A", "a1"), ("C", "c1"), ("D", "d1"), ("F", "f1"), ("F", "f2")])
        b = sc.parallelize([("A", "a2"), ("C", "c2"), ("C", "c3"), ("E", "e1")])
        print(a.join(b).collect())
        print(a.leftOuterJoin(b).collect())
        print(a.rightOuterJoin(b).collect())
        print(a.fullOuterJoin(b).collect())

    def my_sortBy():
        data = [('a',1), ('a',3), ('a',7), ('b',1), ('a',5), ('k',1)]
        rdd1 = sc.parallelize(data)
        #参数1：排序依据  参数2：true升序   参数3：排序分区
        #分区数：1为全局   其余更具业务调整
        rdd2 = rdd1.sortBy(lambda x: x[1], ascending=False, numPartitions=3).collect()
        rdd3 = rdd1.sortBy(lambda x: x[0], ascending=False, numPartitions=3).collect()
        print(rdd2)
        print(rdd3)

    def my_sortByKey():
        data = ["hello spark", "hello world", "hello spark"]
        print(sc.parallelize(data).flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))\
              .reduceByKey(lambda a,b:a+b)\
              .map(lambda x:(x[1],x[0]))\
              .sortByKey(False)\
              .map(lambda x:(x[1],x[0]))\
              .collect())

    def my_action():
        data = [1,2,3,4,5,6,7,8,9,10]
        rdd = sc.parallelize(data)
        print(rdd.collect())
        print(rdd.count())
        print(rdd.take(3))
        print(rdd.max())
        print(rdd.sum())

        print(rdd.reduce(lambda x,y:x+y))

        print(rdd.foreach(lambda x:print(x)))


    my_sortBy()
    # my_reduceByKey()
    sc.stop()



#!/usr/bin/python
# -*- coding:UTF-8 -*-
# @File : job_stu.py
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile("hdfs://stu:9000/data")


    # 统计每个职位投递总次数 & 投递总人数
    # 职位id
    lines_sum = lines.map(lambda line: (line.split(" ")[2], 1)) \
        .reduceByKey(lambda v1, v2: v1 + v2) \
        .sortBy(lambda x: x[1], False)\
        .collect()
    print("每个职位投递总次数:", lines_sum)


    # 每个职位投递总人数
    lines_num = lines.map(lambda line: line.split(" ")[0] + "&" + line.split(" ")[2]) \
        .distinct() \
        .map(lambda line: (line.split("&")[1], 1))\
        .reduceByKey(lambda v1, v2: v1 + v2) \
        .sortBy(lambda x: x[1], False)\
        .collect()
    print("每个职位投递总人数:", lines_num)


    # 统计指定地区的投递的总人数 & 总次数
    lines_count = lines.filter(lambda line: line.split(" ")[1] == "beijing")\
        .map(lambda line:(line.split(" ")[2],1)) \
        .reduceByKey(lambda v1, v2: v1 + v2) \
        .sortBy(lambda x: x[1], False) \
        .collect()
    print("北京地区的岗位投递的总人数:", lines_count)


    # 岗位投递的总次数
    lines_people = lines.filter(lambda line: line.split(" ")[1] == "beijing") \
        .map(lambda line: line.split(" ")[0] + "&" + line.split(" ")[2]) \
        .distinct() \
        .map(lambda line: (line.split("&")[1], 1)) \
        .reduceByKey(lambda v1, v2: v1 + v2) \
        .sortBy(lambda x: x[1], False) \
        .collect()
    print("北京地区的岗位投递的总次数:", lines_people)

    top_job = lines.map(lambda line: ((line.split(" ")[1],  line.split(" ")[2]), 1)) \
            .reduceByKey(lambda v1, v2: v1 + v2) \
            .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .groupByKey() \
            .map(lambda x:(x[0], list(x[1]))) \
            .mapValues(lambda x: sorted(x, key=lambda y:-y[1])[:3]) \
            .collect()
    print("每个地区投递次数最多职位top3", top_job)

