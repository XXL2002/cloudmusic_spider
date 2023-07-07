import re
import jieba
from pyhdfs import HdfsClient
from pyspark import SparkConf, SparkContext

# 读取停用词列表
def stopwordslist():
    stopwords = [line.strip() for line in open('/home/cloudmusic_spider/code/tools/stop_words.txt', 'r', encoding='utf-8').readlines()]
    return set(stopwords)   # 转集合，加快速度


# 增加分词
# def jieba_addwords():
#     jieba.add_word('')


# 对句子进行中文分词
def depart(sentence):

    # 增加指定分词
    # jieba_addwords()

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


def create_dir(client, dir_path):
    if client.exists(dir_path):
        client.delete(dir_path, recursive=True)
    client.mkdirs(dir_path)


# 创建新的目录用以存放分词及词云所需内容
def new_dir(client, new_data_path):

    # 新建data目录
    create_dir(client, new_data_path)

    # 新建info目录
    new_info_path = new_data_path + 'info/'
    create_dir(client, new_info_path)

    # 新建playlist_comments目录
    new_playlist_comments_path = new_data_path + 'playlist_comments/'
    create_dir(client, new_playlist_comments_path)
    
    # 新建song_comments目录
    new_song_comments_path = new_data_path + 'song_comments/'
    create_dir(client, new_song_comments_path)


# 对用户信息文件进行清洗
def user_info_filter(rdd, filepath):

    # 去重、去除列数不为11、对第5列（简介）进行分词，不清洗个人简介为空的用户
    rdd.distinct() \
        .map(lambda line: line.split(' @#$#@ ')) \
        .filter(lambda list: len(list) == 11) \
        .map(lambda list: [list[i] if i != 5 else depart(list[i]) for i in range(len(list))]) \
        .map(lambda list: ' @#$#@ '.join(list)) \
        .saveAsTextFile(filepath)


# 对歌曲信息文件进行清洗
def song_info_filter(rdd, filepath):

    # 去重、去除列数不为6、对第6列（歌词）进行分词
    rdd.distinct() \
        .map(lambda line: line.split(' @#$#@ ')) \
        .filter(lambda list: len(list) == 6) \
        .map(lambda list: [list[i] if i != 5 else lyric_depart(list[i]) for i in range(len(list))]) \
        .map(lambda list: ' @#$#@ '.join(list)) \
        .saveAsTextFile(filepath)


# 对评论文件进行清洗
def comment_filter(rdd, filepath):
    
    # 对评论去重、评论内容分词、去除分词后的无效评论
    rdd.map(lambda line: line.split(' @#$#@ ')) \
        .filter(lambda list: len(list) == 6) \
        .map(lambda list: [list[i] if i != 3 else depart(list[i]) for i in range(len(list))]) \
        .filter(lambda list: list[3] != '') \
        .map(lambda list: ' @#$#@ '.join(list)) \
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
    user_info_filter(rdd1, filepath1)
    
    # 清洗歌曲信息文件
    rdd2 = sc.textFile('hdfs://stu:9000/data/info/song_info.txt')
    filepath2 = '/new_data/info/song_info.txt'
    song_info_filter(rdd2, filepath2)
    
    playlist_files = client.listdir('/data/playlist_comments/')
    playlist_paths = ['/new_data/playlist_comments/' + file for file in playlist_files]
    playlist_rdds = [sc.textFile('hdfs://stu:9000/data/playlist_comments/' + file) for file in playlist_files]

    song_files = client.listdir('/data/song_comments/')
    song_paths = ['/new_data/song_comments/' + file for file in song_files]
    song_rdds = [sc.textFile('hdfs://stu:9000/data/song_comments/' + file) for file in song_files]

    # 清洗歌单评论文件
    for rdd, filepath in zip(playlist_rdds, playlist_paths):
        comment_filter(rdd, filepath)

    # 清洗歌曲评论文件
    for rdd, filepath in zip(song_rdds, song_paths):
        comment_filter(rdd, filepath)