# import sys
# sys.path.append("/root/anaconda3/lib/python3.6/site-packages")
import pymysql
from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient
from utils import showTable, selectAll, deleteAll
from collections import Counter

# def doc_user(filepath,db):
#     conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
#     sc = SparkContext(conf=conf)
#     rdd=sc.textFile(filepath)
#     # rdd.distinct()\
#     rdd_total = rdd.map(lambda line: line.split(" @#$#@ ")) \
#         .map(lambda x: [x[0],x[3],x[4],x[2],x[9]])\
#         .collect()
#     #ID，年龄，地区，性别，emo
#     sql_user(rdd_total,db)



# 9 活跃用户emo指数地区分布表userRegion(省份名 cname, 省份emo指数 cemo)
def userRegion(sc, filepath, connection):
    '''
        filepath: user_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)     # 创建画笔
    rdd = sc.textFile(filepath)   # 读取文件，生成rdd
    
    # 取出用户信息文件中的地区和emo指数
    region_emo = rdd.distinct() \
                    .map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda x: [x[4], float(x[11])]) \
                    .filter(lambda x: x[0] != 'null')
    
    # 为每个区域创建一个累加器，将该区域的所有情感指数累加到累加器中，并计算出该区域的情感指数总和和计数，求得该区域的平均emo指数
    region_avg_emo = region_emo.combineByKey(
                        lambda value: (value, 1),  # 初始值为(emo指数, 1)
                        lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
                        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
                    ).mapValues(lambda acc: acc[0] / acc[1])  # 计算平均值
    
    result = region_avg_emo.collect()

    sql = """INSERT INTO userRegion VALUES(%s,%s)"""
    try:
        cursor.executemany(sql, result)
        connection.commit()   # 提交到数据库执行
    except Exception as e:
        print(e)
        connection.rollback()   # 如果发生错误则回滚

    cursor.close()


# 10 活跃用户emo指数年龄分布表userAge(年龄段 age, 年龄段emo指数 aemo, 年龄段人数 enum)
def userAge(sc, filepath, connection):
    '''
      filepath: user_info.txt
    '''

    rdd = sc.textFile(filepath)
    
    age_dict = {0:'0-9', 1:'10-19', 2:'20-29', 3:'30-39', 4:'40-49', 5:'50-59', 6:'60-69', 7:'70-79', 8:'80-89', 9:'90-99'}

    # 从用户信息表中取出age和emo指数
    age_emo_list = rdd.distinct()\
                      .map(lambda line: line.split(" @#$#@ ")) \
                      .filter(lambda list: list[3] != 'null') \
                      .map(lambda list: (int(list[3])//10, float(list[11]))) \
                      .filter(lambda x: 0 <= x[0] <= 9) \
                      .groupByKey() \
                      .map(lambda x: (x[0], list(x[1]))) \
                      .map(lambda x: (x[0], sum(x[1]) / len(x[1]), len(x[1]))) \
                      .sortBy(lambda x: x[0]) \
                      .map(lambda x: (age_dict[x[0]], x[1], x[2])) \
                      .collect()
    
    
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)
    sql = "INSERT INTO userAge (age, aemo, anum) VALUES (%s, %s, %s)"

    try:
      cursor.executemany(sql, age_emo_list)
      connection.commit()
    except Exception as e:
      print(e)
      connection.rollback()
    
    cursor.close()


# 11 活跃用户emo指数性别分布表userSex(性别 sex, 性别emo指数 semo)
def userSex(sc, filepath, connection):
    '''
        filepath: user_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd = sc.textFile(filepath)

    # 取出性别和emo指数
    region_emo = rdd.distinct() \
                    .map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda x: (x[2], float(x[11]))) \
                    .filter(lambda x: x[0] != 'null')
    
    region_avg_emo = region_emo.combineByKey(
                        lambda value: (value, 1),  # 初始值为(emo指数, 1)
                        lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
                        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
                    ).mapValues(lambda acc: acc[0] / acc[1])  # 计算平均值
    
    result = region_avg_emo.collect()

    sql = """INSERT INTO userSex VALUES(%s,%s)"""
    try:
        cursor.executemany(sql, result)
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 12 活跃用户数据库统计表userNum(活跃用户总数 unum, 收藏歌单总数 cnum, 听歌总数 snum)
def userNum(sc, filepath_user, filepath_playlist, filepath_song, filepath_singer, connection):

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd1 = sc.textFile(filepath_user)
    alluser = rdd1.distinct().count()

    rdd2 = sc.textFile(filepath_playlist)
    allplaylist = rdd2.distinct().count()

    rdd3 = sc.textFile(filepath_song)
    allsong = rdd3.distinct().count()

    rdd4 = sc.textFile(filepath_singer)
    allsinger = rdd4.distinct().count()
    
    try:
        sql = "INSERT INTO userNum (unum, lnum, snum, senum) VALUES (%s, %s, %s, %s)"
        result = (alluser, allplaylist, allsong, allsinger)
        cursor.execute(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 13 活跃用户emo指数前十省份表userTop10City(省份名 cname, 省份emo指数 cemo, 省份人数 cnum)
def userTop10City(sc, filepath, connection):
    '''
        filepath: user_info.txt
    '''
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd = sc.textFile(filepath)

    # 取出用户的省份名、emo指数
    region_emo = rdd.map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda x: (x[4], float(x[11])))
    
    region_avg_count = region_emo.combineByKey(
                            lambda value: (value, 1),  # 初始值为(emo指数, 1)
                            lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
                            lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
                        ).mapValues(lambda acc: (acc[0] / acc[1], acc[1])) \
                         .map(lambda x: (x[0], x[1][0], x[1][1]))

    # 按照平均emo指数降序排序，并取前十个地区
    result = region_avg_count.takeOrdered(10, key=lambda x: -x[1])
    
    sql = 'INSERT INTO userTop10City VALUES(%s,%s,%s)'

    try:
        cursor.executemany(sql, result)     # 注意：插入数据库后记录是无序的
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 15(未验证) 歌单信息收集表listAllNum(歌单id lid, 歌单名 lname, 收藏数 cnum, 评论数 pnum, 播放量 bnum, 收录音乐数 mnum)
def listAllNum(sc, filepath, connetion):
    '''
        filepath: playlist_info.txt
    '''

    cursor = connetion.cursor(cursor = pymysql.cursors.DictCursor)

    rdd = sc.textFile(filepath)

    result = rdd.distinct()\
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list[1], list[3], list[9], list[7]))\
                .collect()

    try:
        sql = "INSERT INTO listAllNum (lid, lname, cnum, pnum, bnum, mnum) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, result)
        connetion.commit()
    except Exception as e:
        print(e)
        connetion.rollback()

    cursor.close()


# 17(未验证) 歌单emo分布表listEmo(歌单id lid, 歌单名 lname, emo指数区间 emo, 出现次数 num)

def count_emo(str_id, song_emo_dict):     # 统计一个歌单处于各个emo区间的歌曲数量
    
    songid_list = str_id.split(' ')   # 歌单包含歌曲id列表
    songEmo_list = [song_emo_dict.get(item, -1) for item in songid_list]  # 获得歌曲的emo指数，不存在置-1
    emo_dict = Counter(int(item / 0.2) for item in songEmo_list if item != -1)

    result = ' '.join([str(value) for value in emo_dict.values()])
    return result


def listEmo(sc, filepath1, filepath2, connection):
    '''
        filepath1: song_info.txt       
        filepath2: playlist_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd1 = sc.textFile(filepath1)

    # 读取歌曲id-emo指数
    song_emo_list = rdd1.distinct()\
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda elems: (elems[0], elems[7]))\
                        .collect()
    
    song_emo_dict = {key:value for key, value in song_emo_list}

    rdd2 = sc.textFile(filepath2)

    # 读取歌单id、歌单名、包含的歌曲id
    result = rdd2.distinct()\
                    .map(lambda line: line.split(" @#$#@ "))\
                    .map(lambda list: (list[0], list[1], "积极 稍积极 中性 稍消极 消极", count_emo(list[7], song_emo_dict))) \
                    .collect()
    
    try:
        sql = "INSERT INTO listEmo (lid, lname, emo, num) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, result)
        connection.commit()
    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 18(未验证) 歌单风格分布表listStyle(歌单id lid, 歌单名 lname, 歌曲风格 style, 出现次数 num)

def count_style(str_id, song_style_dict):       # 统计歌单不同风格以及出现的次数

    songid_list = str_id.split(' ')    # 歌单中歌曲id列表
    style_dict = Counter(style for songid in songid_list for style in song_style_dict.get(songid, []))
    result1 = ' '.join([str(key) for key in style_dict.keys()])
    result2 = ' '.join([str(value) for value in style_dict.values()])
    
    return (result1, result2)


def listStyle(sc, filepath1, filepath2, connection):
    '''
        filepath1: song_info.txt       
        filepath2: playlist_info.txt 
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd1 = sc.textFile(filepath1)
    song_style_list = rdd1.distinct()\
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda elems: (elems[0], elems[7].split(' ')))\
                        .collect()
    
    song_style_dict = {key:value for key, value in song_style_list}

    rdd2 = sc.textFile(filepath2)
    result = rdd2.distinct()\
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list[1], count_style(list[7], song_style_dict))) \
                .map(lambda x: (x[0], x[1], x[2][0], x[2][1]))

    try:
        sql = "INSERT INTO listStyle (lid, lname, style, num) VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 19 歌曲用户性别分布表songSex(歌曲id sid, 歌曲名 sname, 性别 sex, 性别用户数 sex, )
def countGender(rdd, temp, prefix):
    '''
        rdd: user_info.txt rdd
        temp: 
        prefix: 
    '''

    # 取出用户信息文件中的年龄
    sing_count = rdd.map(lambda line: line.split(" @#$#@ ")) \
                        .filter(lambda list: list[0] in temp) \
                        .map(lambda x: x[2]) \
                        .countByKey() \
                        .collect()

    result = [prefix + sublist for sublist in sing_count]
    
    return result


def countAge(rdd,temp,prefix):
    age_filter = rdd.filter(lambda line: line.split(" @#$#@ ")[0] in temp)
    age_emo = age_filter.map(lambda line: line.split(" @#$#@ ")).map(lambda x: (int(x[3]) // 10 * 10, (int(x[11]), 1)))
    age_total_count = age_emo.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    age_avg_count = age_total_count.mapValues(lambda total_count: (total_count[0] / total_count[1], total_count[1]))
    fin = [] 
    for age, (avg_emo, count) in age_avg_count.collect():
        fin.append([age,avg_emo,count])
    result = [prefix + sublist for sublist in fin]
    return result
    

def singersAllNum(filepath,filepath2,db):
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    rdd=sc.textFile(filepath)
    singers_att = rdd.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[3])
    singers_count = singers_att.distinct().count()
    singers_tatt = singers_att.sum()
    sc.stop()
    #获取总歌曲数，直接在song_info里找
    conf2 = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc2 = SparkContext(conf=conf2)
    rdd2=sc2.textFile(filepath2)
    song_all = rdd2.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[0])
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
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    rdd=sc.textFile(filepath)
    singers_style = rdd.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[5])#创作风格现在还没写，默认为尾下标+1
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
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    rdd=sc.textFile(filepath)
    song_ids = rdd.map(lambda line: line.split(" @#$#@ ")).map(lambda x: x[7])
    song_count = song_ids.flatMap(lambda song_ids: [(song_id, 1) for song_id in song_ids])
    song_counts = song_count.reduceByKey(lambda a, b: a + b)
    top_songs = song_counts.takeOrdered(10, key=lambda x: -x[1])
    temp = top_songs.collect()
    sc.stop()#
    #只有ID和出现次数，读取新文件获得歌曲名
    conf2 = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
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
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    rdd=sc.textFile(filepath)
    temp = []
    i = 0
    conft = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sct = SparkContext(conf=conft)
    rddt=sct.textFile(filepath2)
    for item in rdd:
        #前缀，放入函数组成新列表
        prefix = []
        prefix.append(item.split(" @#$#@ ")[0])
        prefix.append(item.split(" @#$#@ ")[1])
        conf2 = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
        sc2 = SparkContext(conf=conf2)
        filepathtemp = "    "+ str(temp[i][0]) + ".txt"#前面的空是地址，自填
        rdd2=sc2.textFile(filepathtemp)
        temp1 = []
        for item2 in rdd2:
            temp1.append(item2.split(" @#$#@ ")[0])
        #函数进行RDD操作
        temp[i]=countGender(rddt,temp1,prefix)
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
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    rdd=sc.textFile(filepath)
    temp = []
    i = 0
    conft = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sct = SparkContext(conf=conft)
    rddt=sct.textFile(filepath2)
    for item in rdd:
        #前缀，放入函数组成新列表
        prefix = []
        prefix.append(item.split(" @#$#@ ")[0])
        prefix.append(item.split(" @#$#@ ")[1])
        conf2 = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
        sc2 = SparkContext(conf=conf2)
        filepathtemp = "    "+ str(temp[i][0]) + ".txt"#前面的空是地址，自填
        rdd2=sc2.textFile(filepathtemp)
        temp1 = []
        for item2 in rdd2:
            temp1.append(item2.split(" @#$#@ ")[0])
        #函数进行RDD操作
        temp[i]=countAge(rddt,temp1,prefix)
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



if __name__=='__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    connection = pymysql.connect(host='762j782l06.zicp.fun',
                                user='root',
                                password='12345678',
                                db='visualData',
                                port=50919,
                                charset='utf8')
    
    table_name = 'listAllNum'

    showTable(connection, table_name)

    # deleteAll(connection, table_name)

    filepath_user = 'hdfs://stu:9000/basic_data/info/user_info.txt'
    filepath_playlist = 'hdfs://stu:9000/basic_data/info/playlist_info.txt'
    filepath_song = 'hdfs://stu:9000/basic_data/info/song_info.txt'
    filepath_singer = 'hdfs://stu:9000/basic_data/info/singer_info.txt'
    # filepath = 'hdfs://stu:9000/emo_data/info/user_info.txt'

    # userRegion(sc, filepath, connection)
    # userAge(sc, filepath, connection)
    # userSex(sc, filepath, connection)
    # userNum(sc, filepath_user, filepath_playlist, filepath_song, filepath_singer, connection)
    # userTop10City(sc, filepath, connection)
    # listAllNum(sc, filepath, connection)

    selectAll(connection, table_name)

    connection.close()
    sc.stop()