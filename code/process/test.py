import os
import re
import shutil
from pyhdfs import HdfsClient
from pyspark import SparkConf, SparkContext

def resetdir(path,clear = False): # local
    if os.path.exists(path):    # 目录已存在
        if clear:
            shutil.rmtree(path)
        return
    os.mkdir(path)
    

def create_dir(client, dir_path, clear = False):   # hdfs
    if client.exists(dir_path):
        if clear:
            client.delete(dir_path, recursive=True)
        return
    client.mkdirs(dir_path)
    

def custom_function(iterable):   

    # 创建HdfsClient对象
    client = HdfsClient(hosts='stu:50070', user_name='root')
    
    tmp = iterable[0].split("/")
    dir, filename = tmp[-2], tmp[-1]    # 文件夹和文件名

    processed_data = "\n".join([' @#$#@ '.join(line.split(' @#$#@ ')) for line in iterable[1].split("\n") if len(line.split(' @#$#@ ')) == 6 and line.split(' @#$#@ ')[3] != '']).encode()
    
    # 将处理后的数据存储回hdfs
    hdfs_dir = f"/new_data/{dir}/"
    hdfs_path = hdfs_dir + filename
    create_dir(client, hdfs_dir)
    client.create(hdfs_path, data=processed_data)

    

if __name__ == '__main__':
    conf = SparkConf().setAppName("CustomFunctionExample")
    sc = SparkContext(conf=conf)
    
    # 重置hdfs的相关文件夹
    client = HdfsClient(hosts='stu:50070', user_name='root')

    create_dir(client,"/new_data/song_comments/",True)

    # 获取HDFS上指定路径下的所有文件
    hdfs_path1 = "hdfs://stu:9000/data/playlist_comments/"
    file_list1 = sc.wholeTextFiles(hdfs_path1)

    hdfs_path2 = "hdfs://stu:9000/data/song_comments/"
    file_list2 = sc.wholeTextFiles(hdfs_path2)

    combined_filelist = file_list1.union(file_list2)
    combined_filelist.foreach(custom_function)
    

    sc.stop()
