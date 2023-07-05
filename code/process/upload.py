# 先安装simplejson，再安装pyhdfs
from pyhdfs import HdfsClient
import os

client = HdfsClient(hosts='stu:50070', user_name='root')

path = '/home/cloudmusic_spider/data/'
remote_path = '/data/'

info_path = path + 'info/'  # info目录路径
playlist_comments_path = path + 'playlist_comments/' # playlist_comments目录路径
song_comments_path = path + 'song_comments/' # song_comments目录路径

remote_info_path = remote_path + 'info/'  # info目录路径
remote_playlist_comments_path = remote_path + 'playlist_comments/' # playlist_comments目录路径
remote_song_comments_path = remote_path + 'song_comments/' # song_comments目录路径

if client.exists(remote_info_path):
    client.delete(remote_info_path, recursive=True)

client.mkdirs(remote_info_path)

if client.exists(remote_playlist_comments_path):
    client.delete(remote_playlist_comments_path, recursive=True)

client.mkdirs(remote_playlist_comments_path)

if client.exists(remote_song_comments_path):
    client.delete(remote_song_comments_path, recursive=True)

client.mkdirs(remote_song_comments_path)


print('开始上传文件...')

for file in os.listdir(info_path):
    path = info_path + file
    hdfs_path = remote_info_path + file
    client.copy_from_local(path, hdfs_path)

for file in os.listdir(playlist_comments_path):
    path = playlist_comments_path + file
    hdfs_path = remote_playlist_comments_path + file
    client.copy_from_local(path, hdfs_path)

for file in os.listdir(song_comments_path):
    path = song_comments_path + file
    hdfs_path = remote_song_comments_path + file
    client.copy_from_local(path, hdfs_path)   

print('上传完毕')