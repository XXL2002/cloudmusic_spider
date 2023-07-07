from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient


# 提取评论中的地区信息和单词
def extract_words(words, region):
    region = region.strip()
    word_list = words.split(" ")
    return [(region, word) for word in word_list]

# 统计完每个地区的高频词汇后整理
def transform(region, list):
    words = ''.join([word[0] for word in list])
    frequences = ''.join([word[1] for word in list])
    return [region, words, frequences]

# 统计一个歌曲评论文件中的词频
def province_word_count(sc):

    # 返回是键值对的形式，键-文件路径，值-文件内容（字符串）
    # playlist_rdd = sc.wholeTextFiles('hdfs://stu:9000/new_data/playlist_comments/*').flatMap(lambda x: x[1].split('\n'))
    # song_rdd = sc.wholeTextFiles('hdfs://stu:9000/new_data/song_comments/*').flatMap(lambda x: x[1].split('\n'))
    
    playlist_comments_rdd = sc.textFile("hdfs://stu:9000/new_data/playlist_comments/*.txt")
    song_comments_rdd = sc.textFile("hdfs://stu:9000/new_data/song_comments/*.txt")

    # 合并两个rdd
    rdd = playlist_comments_rdd.union(song_comments_rdd)
    
    # rdd1 = rdd.map(lambda line: line.split(' @#$#@ ')) \
    #             .filter(lambda list: list[5] != 'null') \
    #             .flatMap(lambda list: extract_words(list[3], list[5])) \
    #             .map(lambda x: (x, 1)) \
    #             .reduceByKey(lambda a, b: a + b) \
    #             .collect()

    rdd1 = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .filter(lambda list: list[5] != 'null') \
                .flatMap(lambda list: extract_words(list[3], list[5])) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                .groupByKey() \
                .map(lambda x:(x[0], list(x[1]))) \
                .mapValues(lambda x: sorted(x, key=lambda y:-y[1])[:10]) \
                .map(lambda x: transform(x[0], x[1])) \
                .map(lambda x: ' @#$#@ '.join(x)) \
                .collect()
    
    print(rdd1)
    # 求取每个地区的前十个词频
    # rdd  = rdd.map(lambda line: line.split(" @#$#@ ")) \
    #             .map(lambda list: (list[5], list[4])) \
    #             .groupByKey() \
    #             .map(lambda x:(x[0], list(x[1]))) \
                
                


if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    province_word_count(sc)



