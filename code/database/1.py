from pyspark import SparkConf, SparkContext
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import Row


# 10 活跃用户emo指数年龄分布表(年龄段 age, 年龄段emo指数 aemo, 年龄段人数 enum)
def userAge(filepath, connect):
    '''
      filepath: user_info.txt
      db: 数据库连接
    '''

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(filepath)
    
    # 从用户信息表中取出age和emo指数
    age_emo_list = rdd.distinct()\
                      .map(lambda line: line.split(" @#$#@ ")) \
                      .filter(lambda list: list[3] != 'null') \
                      .map(lambda list: (int(list[3])//10, float(list[11]))) \
                      .groupByKey() \
                      .map(lambda x: (x[0], list(x[1]))) \
                      .map(lambda x: (x[0], sum(x[1]) / len(x[1]), len(x[1]))) \
                      .sortBy(lambda x: x[0]) \
                      .collect()
    
    
    cursor = connect.cursor(cursor = pymysql.cursors.DictCursor)
    sql = "INSERT INTO userAge (age, aemo, enum) VALUES (%s, %s, %s)"

    try:
      cursor.executemany(sql, age_emo_list)
      connect.commit()
    except Exception as e:
      print(e)
      connect.rollback()
    
    sc.stop()
    cursor.close()
    connect.close()




def total_user_info(filepath,db):# 插入数据到表userNum
  # filepath: user_info.txt
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  sc = SparkContext(conf=conf)
  lines=sc.textFile(filepath)

  line_song_info=lines.distinct()\
                      .map(lambda line: line.split(" @#$#@ "))\
                      .map(lambda list: [list[6], list[10].split(" ")])\
                      .map(lambda list: [list[0], count(list[1])])\
                      .collect()
  # 听歌数和收藏歌单数
  
  count_user=0
  count_listenSongs=0
  count_collect_play=0
  for item in line_song_info:
    count_user=count_user+1
    count_listenSongs=count_listenSongs+int(item[0])
    count_collect_play=count_collect_play+int(item[1])
  
  # 使用cursor()方法获取操作游标 
  cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
  # 插入数据到表userNum
  try:
    sql = "INSERT INTO userNum (unum, cnum, snum) VALUES (%s, %s, %s)"
    data = (count_user, count_collect_play, count_listenSongs)
    cursor.execute(sql, data)
    db.commit()
  except Exception as e:
    print(e)
    # 如果发生错误则回滚
    db.rollback()
  cursor.close()
  db.close()

def count(list1):
  if(list1[0]=="null"):
    return 0
  else:
    return len(list1)
  

def singer_info(filepath, db):  # 插入数据到表singersNumInfo
    # filepath: singer_info.txt
    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile(filepath)

    line_singer_info=lines.distinct()\
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda list: [list[0], list[1], list[3],list[5]])\
                        .collect()
    # 歌手id、歌手名、关注量、emo指数

    # 使用cursor()方法获取操作游标 
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
    # 插入数据到表singersNumInfo
    for item in line_singer_info:
      try:
        sql = "INSERT INTO singerAllNum (sid, sname, cnum, emo) VALUES (%s, %s, %s, %s)"
        data = (item[0], item[1], item[2], item[3])
        cursor.execute(sql, data)
        db.commit()
      except Exception as e:
        print(e)
        # 如果发生错误则回滚
        db.rollback()
    cursor.close()
    db.close()


def playlist_info(filepath,db): # 插入数据到表songListsNumInfo
  # filepath: playlist_info.txt
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  sc = SparkContext(conf=conf)
  lines=sc.textFile(filepath)

  line_playlist_info=lines.distinct()\
                      .map(lambda line: line.split(" @#$#@ "))\
                      .map(lambda list: [list[0], list[1], list[2], list[3], list[8], list[9]])\
                      .collect()
  # 歌单id、歌单名、歌单播放量、歌单收藏量、歌单评论量、歌单emo指数
  # 使用cursor()方法获取操作游标 
  cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
  # 插入数据到表songListsNumInfo
  for item in line_playlist_info:
    try:
      sql = "INSERT INTO songListsNumInfo (lid, lname, snum, cnum, pnum, lemo) VALUES (%s, %s, %s, %s, %s, %s)"
      data = (item[0], item[1], item[2], item[3], item[4], item[5])
      cursor.execute(sql, data)
      db.commit()
    except Exception as e:
      print(e)
      # 如果发生错误则回滚
      db.rollback()
  cursor.close()
  db.close()


def playlist_all_info(filepath,filepath1,db): # 插入数据到表songListAllNum
  #filepath：playlist_info.txt、 filepath1: song_info.txt
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  sc = SparkContext(conf=conf)
  lines=sc.textFile(filepath)

  line_playlist_info=lines.distinct()\
                      .map(lambda line: line.split(" @#$#@ "))\
                      .map(lambda list: [list[3], list[8], list[2]])\
                      .collect()
  # 歌单收藏数、歌单评论数、歌单播放量、  
  
  # 歌曲数
  conf1 = SparkConf().setMaster("spark://stu:7077").setAppName("job02")
  sc1 = SparkContext(conf=conf1)
  lines1=sc1.textFile(filepath1)

  songs=lines1.distinct().count()

  subscribedCount=0
  comment=0
  playCount=0
  for item in line_playlist_info:
    subscribedCount=subscribedCount+item[0]
    comment=comment+item[1]
    playCount=playCount+item[2]
  # 打开数据库连接
  db = pymysql.connect(host="124.222.244.117", user="zrgj2", password="zrgj2", database="zrgj2")
  # 使用cursor()方法获取操作游标 
  cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
  # 插入数据到表songListAllNum
  try:
    sql = "INSERT INTO songListAllNum (allcollect, allcomment, allplay, allmusicnum) VALUES (%s, %s, %s, %s, %s, %s)"
    data = (subscribedCount, comment, playCount, songs)
    cursor.execute(sql, data)
    db.commit()
  except Exception as e:
    print(e)
    # 如果发生错误则回滚
    db.rollback()
  cursor.close()
  db.close()


def playlist_user_region(filepath, filepath1, db):   # 插入数据到表songListRegion
  # filepath: playlist_info.txt; filepath1: playlist_XXXXXX.txt所在文件夹路径+"playlist_"
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  sc = SparkContext(conf=conf)
  lines=sc.textFile(filepath)

  line_playlist_user_info=lines.distinct()\
                      .map(lambda line: line.split(" @#$#@ "))\
                      .map(lambda list: [list[0], list[1]])\
                      .collect()
  # 歌单id、歌单名字
  
  provinces = ["北京市", "天津市", "河北省", "山西省", "内蒙古自治区",
      "辽宁省", "吉林省", "黑龙江省", "上海市", "江苏省",
      "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省",
      "湖北省", "湖南省", "广东省", "广西壮族自治区", "海南省",
      "重庆市", "四川省", "贵州省", "云南省", "西藏自治区",
      "陕西省", "甘肃省", "青海省", "宁夏回族自治区", "新疆维吾尔自治区", 
      "香港", "澳门" ,"台湾"]
  # 打开数据库连接
  db = pymysql.connect(host="124.222.244.117", user="zrgj2", password="zrgj2", database="zrgj2")
  # 使用cursor()方法获取操作游标 
  cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
  # 插入数据到表songListRegion
  for item in line_playlist_user_info:
    user_count=[0]*34
    conf1 = SparkConf().setMaster("spark://stu:7077").setAppName("job02")
    sc1 = SparkContext(conf=conf1)
    lines=sc1.textFile(filepath1+line_playlist_user_info[0]+".txt")

    line_playlist_user_comment_info=lines.distinct()\
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda list: [list[0], list[6]])\
                        .collect()
    # 评论用户id,评论发表地区
    for item1 in line_playlist_user_comment_info:
      for i in range(34):
        if item1[1]==provinces[i]:
          user_count[i]=user_count[i]+1
          break
      for i in range(34):
        try:
          sql = "INSERT INTO songListRegion (lid, lname, sex, num) VALUES (%s, %s, %s, %s)"
          data = (item[0], item[1], provinces[i], user_count[i])
          cursor.execute(sql, data)
          db.commit()
        except Exception as e:
          print(e)
          # 如果发生错误则回滚
          db.rollback()
  cursor.close()
  db.close()

def playlist_user_age(filepath, filepath1, db):  # 插入数据到表songListAge
  # filepath: playlist_info.txt; filepath1: playlist_XXXXXX.txt所在文件夹路径+"playlist_"
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  sc = SparkContext(conf=conf)
  lines=sc.textFile(filepath)

  line_playlist_user_info=lines.distinct()\
                      .map(lambda line: line.split(" @#$#@ "))\
                      .map(lambda list: [list[0], list[1]])\
                      .collect()
  # 歌单id、歌单名字
  user_total=[0]*10
  emo_total=[0]*10

  # 打开数据库连接
  db = pymysql.connect(host="124.222.244.117", user="zrgj2", password="zrgj2", database="zrgj2")
  # 使用cursor()方法获取操作游标 
  cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
  # 插入数据到表songListAge
  for item in line_playlist_user_info:
    conf1 = SparkConf().setMaster("spark://stu:7077").setAppName("job02")
    sc1 = SparkContext(conf=conf1)
    lines1=sc1.textFile(filepath1+item[0]+".txt")
    line_comment_info=lines1.distinct()\
                            .map(lambda line: line.split(" @#$#@ "))\
                            .map(lambda list: [list[0]])\
                            .map(lambda list:[get_emo[list[0], get_age[list[0]]]])\
                            .collect()
    for item1 in line_comment_info:
      for i in range(9):
        if (i*10)<=int(item1[1]) and (i*10+9)>=int(item1[1]):
          user_total[i]=user_total[i]+1
          emo_total[i]=emo_total[i]+item1[0]
          break
        elif i==8:
          user_total[9]=user_total[9]+1
          emo_total[9]=emo_total[9]+item1[0]
    for i in range(len(emo_total)):
      if(user_total[i]!=0):
        emo_total[i]=emo_total[i]/user_total[i]
      try:
        sql = "INSERT INTO songListAge (lid, lname, age, emo, num) VALUES (%s, %s, %s, %s, %s)"
        if i<=8:
          data = (item[0], item[1], f"{i*10}-{i*10+9}",  emo_total[i], user_total[i])
        else:
          data = ("else", user_total[i], emo_total[i])
        cursor.execute(sql, data)
        db.commit()
      except Exception as e:
        print(e)
      # 如果发生错误则回滚
        db.rollback()
  cursor.close()
  db.close()

def get_emo(id):
  # filepath="hdfs://stu:9000/data/info/user_info.txt"
  # conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  # sc = SparkContext(conf=conf)
  # lines=sc.textFile(filepath)

  # line_user_info=lines.distinct()\
  #                     .map(lambda line: line.split(" "))\
  #                     .map(lambda list: [list[0], list[3],list[11]])\
  #                     .collect()
  # # id, age, emo

  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job03")
    
  spark = SparkSession.builder.config(conf=conf).getOrCreate()
  sc = spark.sparkContext
  sc.setLogLevel("WARN")
  rdd_file = sc.textFile("hdfs://stu:9000/data/info/user_info.txt")
  lines_user_info = rdd_file.distinct()\
                              .map(lambda record: record.split(","))\
                              .map(lambda list: list[list[0],list[3],list[11]])\
                              .collect()
  df = lines_user_info.map(lambda line: Row(user_id=line[0], user_age=int(line[1]), user_emo=line[2])).toDF()
  df.createOrReplaceTempView("t_table")
  emo=spark.sql("select user_emo from t_table where t_table.user_id="+str(id))
  return emo

def get_age():
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job03")
    
  spark = SparkSession.builder.config(conf=conf).getOrCreate()
  sc = spark.sparkContext
  sc.setLogLevel("WARN")
  rdd_file = sc.textFile("hdfs://stu:9000/data/info/user_info.txt")
  lines_user_info = rdd_file.distinct()\
                              .map(lambda record: record.split(","))\
                              .map(lambda list: list[list[0],list[3],list[11]])\
                              .collect()
  df = lines_user_info.map(lambda line: Row(user_id=line[0], user_age=int(line[1]), user_emo=line[2])).toDF()
  df.createOrReplaceTempView("t_table")
  age=spark.sql("select user_age from t_table where t_table.user_id="+str(id))
  return age


def playlist_all_info(filepath,db): # 插入数据到表songListNum
  #  filepath: playlist_info.txt;
  conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
  sc = SparkContext(conf=conf)
  lines=sc.textFile(filepath)

  line_playlist_user_info=lines.distinct()\
                      .map(lambda line: line.split(" @#$#@ "))\
                      .map(lambda list: [list[0], list[1], list[3], list[8], list[2]])\
                      .collect()
  # 歌单id、歌单名字、收藏数、评论量、播放量、收录音乐

  # 打开数据库连接
  db = pymysql.connect(host="124.222.244.117", user="zrgj2", password="zrgj2", database="zrgj2")
  # 使用cursor()方法获取操作游标 
  cursor = db.cursor(cursor = pymysql.cursors.DictCursor)
  # 插入数据到表songListNum

  for item in line_playlist_user_info:
    try:
      sql = "INSERT INTO songListNum (lid, lname, cnum, pnum, snum, mnum) VALUES (%s, %s, %s, %s, %s, %s)"
      data = (item[0], item[1], item[2], item[3], item[4], item[5], 100)
      cursor.execute(sql, data)
      db.commit()
    except Exception as e:
      print(e)
      # 如果发生错误则回滚
      db.rollback()
  cursor.close()
  db.close()


if __name__=="__main__":

    # connect = pymysql.connect(host='762j782l06.zicp.fun',
    #                           user='root',
    #                           password='12345678',
    #                           db='visualData',
    #                           port=51825,
    #                           charset='utf8')
    
    filepath = 'hdfs://stu:9000/emo_data/info/user_info.txt'
    userAge(filepath)
  
