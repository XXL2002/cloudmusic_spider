from pyhdfs import HdfsClient
client = HdfsClient(hosts='stu:50070',user_name='root')
client.copy_from_local('/home/hadoop1/software/data/one.txt','/1')#本地文件绝对路径,HDFS目录必须不存在