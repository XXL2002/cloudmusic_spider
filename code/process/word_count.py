from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient


# 统计一个歌曲评论文件中的词频
def word_count(sc, song_id):
    
    filepath1 = f'/new_data/song_comments/song_{song_id}.txt'   # 歌曲评论文件
    rdd = sc.textFile('hdfs://stu:9000' + filepath1)        # 获取rdd
    rdd_result = rdd.map(lambda line: line[3]) \
                    .collect()
    print(rdd_result)


if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    word_count(sc, )



