import sys
sys.path.append("/root/anaconda3/lib/python3.6/site-packages")
from pyspark import SparkConf, SparkContext
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

# def doc_user(filepath,db):
#     conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
#     sc = SparkContext(conf=conf)
#     lines=sc.textFile(filepath)
#     # lines.distinct()\
#     lines_total = lines.map(lambda line: line.split(" @#$#@ ")) \
#         .map(lambda x: [x[0],x[3],x[4],x[2],x[9]])\
#         .collect()
#     #ID，年龄，地区，性别，emo
#     sql_user(lines_total,db)

# 9 活跃用户emo指数地区分布表(省份名 cname, 省份emo指数 cemo)
def userRegion(filepath, db):
    
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    region_emo = lines.distinct().map(lambda line: line.split(" @#$#@ ")).map(lambda x: [x[4], float(x[11])])
    region_avg_emo = region_emo.combineByKey(
    lambda value: (value, 1),  # 初始值为(emo指数, 1)
    lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
    ).mapValues(lambda acc: acc[0] / acc[1])  # 计算平均值
    temp = region_avg_emo.collect()
    sql = """INSERT INTO userRegion VALUES(%s,%s)"""
    try:
        cursor.executemany(sql,temp)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc.stop()
    cursor.close()
    db.close()


def countGender(lines,temp,prefix):
    singers_style = lines.map(lambda line: line.split(" @#$#@ ")).filter(lambda v:v[0] in temp).map(lambda x: x[2])
    singers_count = singers_style.countByKey()
    sing_count = singers_count.collect()
    result = [prefix + sublist for sublist in sing_count]
    return result

def countAge(lines,temp,prefix):
    age_filter = lines.filter(lambda line: line.split(" @#$#@ ")[0] in temp)
    age_emo = age_filter.map(lambda line: line.split(" @#$#@ ")).map(lambda x: (int(x[3]) // 10 * 10, (int(x[11]), 1)))
    age_total_count = age_emo.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    age_avg_count = age_total_count.mapValues(lambda total_count: (total_count[0] / total_count[1], total_count[1]))
    fin = [] 
    for age, (avg_emo, count) in age_avg_count.collect():
        fin.append([age,avg_emo,count])
    result = [prefix + sublist for sublist in fin]
    return result

def userSex(filepath,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    region_emo = lines.map(lambda line: line.split(" @#$#@ ")).map(lambda x: (x[2], float(x[11])))
    region_avg_emo = region_emo.combineByKey(
    lambda value: (value, 1),  # 初始值为(emo指数, 1)
    lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
    ).mapValues(lambda acc: acc[0] / acc[1])  # 计算平均值
    temp = region_avg_emo.collect()
    sql = """INSERT INTO userSex VALUES(%s,%s)"""
    try:
        cursor.executemany(sql,temp)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc.stop()
    cursor.close()
    db.close()

def userTop10City(filepath,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    region_emo = lines.map(lambda line: line.split(" @#$#@ ")).map(lambda x: (x[4], float(x[11])))
    region_avg_count = region_emo.combineByKey(
    lambda value: (value, 1),  # 初始值为(emo指数, 1)
    lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
    ).mapValues(lambda acc: (acc[0] / acc[1], acc[1]))  # 计算平均值和计数
    # 按照平均emo指数降序排序，并取前十个地区
    top_10_regions = region_avg_count.takeOrdered(10, key=lambda x: -x[1][0])
    fin = [] 
    for city, (avg_emo, count) in top_10_regions:
        fin.append([city,avg_emo,count])
    sql = """INSERT INTO userTop10City VALUES(%s,%s,%s)"""
    for item in fin:
      try:
          cursor.execute(sql,item)
      # 提交到数据库执行
          db.commit()
      except Exception as e:
          print(e)
      # 如果发生错误则回滚
          db.rollback()
    sc.stop()
    cursor.close()
    db.close()
    

def singersAllNum(filepath,filepath2,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    singers_att = lines.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[3])
    singers_count = singers_att.distinct().count()
    singers_tatt = singers_att.sum()
    sc.stop()
    #获取总歌曲数，直接在song_info里找
    conf2 = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc2 = SparkContext(conf=conf2)
    lines2=sc2.textFile(filepath2)
    song_all = lines2.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[0])
    song_total = song_all.distinct().count()
    sql = """INSERT INTO singersAllNum VALUES(%s,%s,%s)"""
    #按照只有一个元素来填的
    try:
        cursor.executemany(sql,singers_tatt,singers_count,song_total)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc2.stop()
    cursor.close()
    db.close()

def singersStyle(filepath,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    singers_style = lines.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[5])#创作风格现在还没写，默认为尾下标+1
    singers_count = singers_style.countByKey()
    temp = singers_count.collect()
    sql = """INSERT INTO singersAllNum VALUES(%s,%s)"""
    try:
        cursor.executemany(sql,temp)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc.stop()
    cursor.close()
    db.close()

def songListsMostSong(filepath,filepath2,db):
    tempf = []
    #取收录前十的歌曲
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    song_ids = lines.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[7])
    song_count = song_ids.flatMap(lambda song_ids: [(song_id, 1) for song_id in song_ids])
    song_counts = song_count.reduceByKey(lambda a, b: a + b)
    top_songs = song_counts.takeOrdered(10, key=lambda x: -x[1])
    temp = top_songs.collect()
    sc.stop()#
    #只有ID和出现次数，读取新文件获得歌曲名
    conf2 = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc2 = SparkContext(conf=conf2)
    line2=sc2.textFile(filepath2)
    song_data = line2.map(lambda line: line.split("\t")).map(lambda x: (x[0], x[1]))
    song_count2 = sc.parallelize(temp)
    filtered_song = song_data.join(song_count2).filter(lambda x: x[0] in temp)
    i = 0
    for song_id, (song_name, count) in filtered_song.collect():
        tempf[i]=[song_id,song_name,count]
        i += 1
    sql = """INSERT INTO singersAllNum VALUES(%s,%s,%s)"""
    try:
        cursor.executemany(sql,tempf)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc2.stop()
    cursor.close()
    db.close()

def songListSex(filepath,filepath2,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    temp = []
    i = 0
    conft = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sct = SparkContext(conf=conft)
    linest=sct.textFile(filepath2)
    for item in lines:
        #前缀，放入函数组成新列表
        prefix = []
        prefix.append(item.split(" @#$#@ ")[0])
        prefix.append(item.split(" @#$#@ ")[1])
        conf2 = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
        sc2 = SparkContext(conf=conf2)
        filepathtemp = "    "+ str(temp[i][0]) + ".txt"#前面的空是地址，自填
        lines2=sc2.textFile(filepathtemp)
        temp1 = []
        for item2 in lines2:
            temp1.append(item2.split(" @#$#@ ")[0])
        #函数进行RDD操作
        temp[i]=countGender(linest,temp1,prefix)
    sql = """INSERT INTO singersAllNum VALUES(%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql,temp)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc.stop()
    sct.stop()
    sc2.stop()
    cursor.close()
    db.close()

def songListAge(filepath,filepath2,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)
    temp = []
    i = 0
    conft = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sct = SparkContext(conf=conft)
    linest=sct.textFile(filepath2)
    for item in lines:
        #前缀，放入函数组成新列表
        prefix = []
        prefix.append(item.split(" @#$#@ ")[0])
        prefix.append(item.split(" @#$#@ ")[1])
        conf2 = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
        sc2 = SparkContext(conf=conf2)
        filepathtemp = "    "+ str(temp[i][0]) + ".txt"#前面的空是地址，自填
        lines2=sc2.textFile(filepathtemp)
        temp1 = []
        for item2 in lines2:
            temp1.append(item2.split(" @#$#@ ")[0])
        #函数进行RDD操作
        temp[i]=countAge(linest,temp1,prefix)
    sql = """INSERT INTO singersAllNum VALUES(%s,%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql,temp)
    # 提交到数据库执行
        db.commit()
    except Exception as e:
        print(e)
    # 如果发生错误则回滚
        db.rollback()
    sc.stop()
    sct.stop()
    sc2.stop()
    cursor.close()
    db.close()