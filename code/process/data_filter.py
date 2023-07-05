# -*- coding:UTF-8 -*-
import sys
sys.path.append("code")

from pyspark import SparkConf, SparkContext
from process.word_cut import comment_depart, lyric_depart


# 对评论文件进行清洗
def comment_filter(filepath):

    rdd = sc.textFile(filepath)

    # 对评论去重、评论内容分词、去除分词后的无效评论
    return rdd.distinct() \
            .map(lambda line: line.split(',')) \
            .map(lambda list: [list[i] if i != 3 else comment_depart(list[i]) for i in range(len(list))]) \
            .filter(lambda list: list[3] != '') \
            .collect()


# 对歌曲信息文件进行清洗
def song_info_filter(filepath):

    rdd = sc.textFile(filepath)

    # 去除歌曲前后不必要的信息
    return rdd.distinct() \
                .map(lambda line: line.split(',')) \
                .map(lambda list: [list[i] if i != 4 else lyric_depart(list[i]) for i in range(len(list))]) \
                .collect()



if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    # filepath = 'hdfs://stu:9000/data/playlist_comments/playlist_19723756.txt'
    # rdd_result = comment_filter(filepath)
    filepath = 'hdfs://stu:9000/data/info/song_info.txt'
    rdd_result = song_info_filter(filepath)
    print(rdd_result)
    
    
    