import re
import jieba
from pyhdfs import HdfsClient
from pyspark import SparkConf, SparkContext


# 读取停用词列表
def stopwordslist():
    stopwords = [line.strip() for line in open('/home/cloudmusic_spider/code/tools/stop_words.txt', 'r', encoding='utf-8').readlines()]
    return set(stopwords)   # 转集合，加快速度


# 增加分词
def jieba_addwords():
    jieba.add_word('')


# 对句子进行中文分词
def depart(sentence):

    # 增加指定分词
    jieba_addwords()

    # 进行中文分词
    word_list = jieba.lcut(sentence.strip())
    
    # 读取停用词列表
    stopwords = stopwordslist()

    # 筛选
    result = [word for word in word_list if word not in stopwords and len(word) > 1]

    # 返回最终结果，空格分割的字符串
    return ' '.join(result)


def lyric_depart(lyric):

    lyric = re.sub('\[[^\]]*\]', '', lyric)    # 去除时间标签
    lyric = re.sub(r'\s*\S+\s*[：:]\s*\S+', '', lyric).lstrip()

    return depart(lyric)


# 创建新的目录用以存放分词及词云所需内容
def new_dir(client, new_data_path):

    # 新建data目录
    if client.exists(new_data_path):
        client.delete(new_data_path, recursive=True)
    client.mkdirs(new_data_path)

    # 新建info目录
    new_info_path = new_data_path + 'info/'
    if client.exists(new_info_path):
        client.delete(new_info_path, recursive=True)
    client.mkdirs(new_info_path)

    # 新建playlist_comments目录
    new_playlist_comments_path = new_data_path + 'playlist_comments/'
    if client.exists(new_playlist_comments_path):
        client.delete(new_playlist_comments_path, recursive=True)
    client.mkdirs(new_playlist_comments_path)

    # 新建song_comments目录
    new_song_comments_path = new_data_path + 'song_comments/'
    if client.exists(new_song_comments_path):
        client.delete(new_song_comments_path, recursive=True)
    client.mkdirs(new_song_comments_path)


# 对用户信息文件进行清洗
def user_info_filter(rdd, filepath):

    return rdd.distinct() \
                .map(lambda line: line.split(',')) \
                .map(lambda list: [list[i] if i != 4 else depart(list[i]) for i in range(len(list))]) \
                .saveAsTextFile(filepath)


# 对歌曲信息文件进行清洗
def song_info_filter(rdd, filepath):

    # 去除歌曲前后不必要的信息
    rdd.distinct() \
        .map(lambda line: line.split(',')) \
        .map(lambda list: [list[i] if i != 5 else lyric_depart(list[i]) for i in range(len(list))]) \
        .saveAsTextFile(filepath)


# 对评论文件进行清洗
def comment_filter(rdd, filepath):
    
    # 对评论去重、评论内容分词、去除分词后的无效评论
    return rdd.map(lambda line: line.split(',')) \
            .map(lambda list: [list[i] if i != 3 else depart(list[i]) for i in range(len(list))]) \
            .filter(lambda list: list[3] != '') \
            .saveAsTextFile(filepath)



if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    # 创建new_data目录
    new_dir(client, '/new_data/')

    # 清洗用户信息文件
    rdd1 = sc.textFile('hdfs://stu:9000/data/info/user_info.txt')
    filepath1 = '/new_data/info/user_info.txt'
    rdd_result1 = user_info_filter(rdd1, filepath1)

    # 清洗歌曲信息文件
    rdd2 = sc.textFile('hdfs://stu:9000/data/info/song_info.txt')
    filepath2 = '/new_data/info/song_info.txt'
    rdd_result2 = song_info_filter(rdd2, filepath2)

    # 清洗歌单评论文件
    file_list = client.listdir('/data/playlist_comments/')
    for filepath in file_list:

        filepath3 = 'hdfs://stu:9000/data/playlist_comments/' + filepath
        rdd3 = sc.textFile(filepath3)
        # rdd_result3 = comment_filter(rdd3)

    # 清洗歌曲评论文件
    file_list = client.listdir('/data/song_comments/')
    for filepath in file_list:
        
        # 清洗评论文件
        filepath4 = 'hdfs://stu:9000/data/song_comments/' + filepath
        rdd4 = sc.textFile(filepath4)
        # rdd_result4 = comment_filter(rdd4)