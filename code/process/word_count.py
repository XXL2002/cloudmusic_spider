import sys
sys.path.append('code')

import re
import pymysql
from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient
from tools.struct import Music_charts


# 提取评论中的地区信息和单词
def extract_words(words, region):
    region = region.strip()
    word_list = words.split(" ")
    return [(region, word) for word in word_list]

# 统计完每个地区的高频词汇后整理
def transform(region, list):
    words = ' '.join([word[0] for word in list])
    frequences = ' '.join([str(word[1]) for word in list])
    return (region, words, frequences)


# 统计每个地区评论中的词频
def province_word_count(sc, connection):
    
    playlist_comments_rdd = sc.textFile("hdfs://stu:9000/new_data/playlist_comments/*.txt")
    song_comments_rdd = sc.textFile("hdfs://stu:9000/new_data/song_comments/*.txt")
    
    # 合并两个rdd
    rdd = playlist_comments_rdd.union(song_comments_rdd)

    # 过滤地区不存在、提取评论与地区、按地区统计词频
    result = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .filter(lambda list: list[5] != 'null') \
                .flatMap(lambda list: extract_words(list[3], list[5])) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                .groupByKey() \
                .map(lambda x:(x[0], list(x[1]))) \
                .mapValues(lambda x: sorted(x, key=lambda y:-y[1])[:10]) \
                .map(lambda x: transform(x[0], x[1])) \
                .collect()
    
    cursor = connection.cursor()
    
    # 14 活跃用户地区评论词云表userWord(省份名 cname, 评论词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO userWord (cname, word, cnt) VALUES (%s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    cursor.close()
    connection.close()


# 统计每个歌单评论中的词频
def playlist_word_count(sc, client, connection, Music_charts):

    inverse_dict = {str(value): key for key, value in Music_charts.items()}

    # 歌单名列表
    playlist_files = client.listdir('/cut_data/playlist_comments/')

    # 歌单rdd列表
    playlist_rdds = [sc.textFile(f'hdfs://stu:9000/cut_data/playlist_comments/{file}') for file in playlist_files]

    result = []
    for i in range(len(playlist_rdds)):

        word_list = playlist_rdds[0].map(lambda line: line.split(' @#$#@ ')) \
                                    .filter(lambda list: len(list) == 6) \
                                    .flatMap(lambda list: list[3].split(' ')) \
                                    .map(lambda x: (x, 1)) \
                                    .reduceByKey(lambda a, b: a + b) \
                                    .sortBy(lambda tuple: tuple[1], ascending=False) \
                                    .take(10)
                    
        id = re.search(r'\d+', playlist_files[i]).group(0)
        words = ' '.join([word[0] for word in word_list])
        frequence = ' '.join([str(word[1]) for word in word_list])
        result.append((id, inverse_dict[id], words, frequence))
        
        
    cursor = connection.cursor()

    # 16 歌单评论词云表listCommentWord(歌单id lid, 歌单名 lname, 词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO listCommentWord (lid, lname, word, cnt) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    cursor.close()
    connection.close()


# 爬取每个歌手创建歌曲时的高频词汇
def singer_word_count(sc, connection):
    
    # 从歌手信息表中取出歌手id、歌手名、歌手创作歌曲id
    singer_list = sc.textFile('hdfs://stu:9000/data/info/singer_info.txt') \
            .map(lambda line: line.split(' @#$#@ ')) \
            .filter(lambda list: len(list) == 5) \
            .map(lambda list: [list[0], list[1], list[4].split(' ')]) \
            .collect()
    
    result = []
    rdd = sc.textFile('hdfs://stu:9000/new_data/info/song_info.txt') \
            .map(lambda line: line.split(' @#$#@ ')) \
            .filter(lambda list: len(list) == 6)
    
    # 遍历每个歌手
    for singer in singer_list:

        word_list = rdd.filter(lambda list: list[0] in singer[2]) \
                        .flatMap(lambda list: list[5].split(' ')) \
                        .map(lambda x: (x, 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda tuple: tuple[1], ascending=False) \
                        .take(10)

        words = ' '.join([word[0] for word in word_list])
        frequence = ' '.join([str(word[1]) for word in word_list])

        result.append((singer[0], singer[1], words, frequence))
    
    cursor = connection.cursor()

    # 29 歌手歌曲歌词词云表singerWord(歌手id seid, 歌手名 sename, 所有歌词词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO singerWord (seid, sename, word, cnt) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    cursor.close()
    connection.close()
        
          



if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    connection = pymysql.connect(host='762j782l06.zicp.fun',
                                user='root',
                                password='12345678',
                                db='visualData',
                                port=50919,
                                charset='utf8')


    # # 统计地区词云
    # province_word_count(sc, connection)

    # # 统计歌单词云
    # playlist_word_count(sc, client, connection, Music_charts)

    # # 统计歌手创作词云
    # singer_word_count(sc, connection)

    connection.close()
    sc.stop()



