#! /usr/bin/env python
# coding='utf-8'

import math
from utils import get, save_csv

# 获取热评
def hotcomments(content_json, songname, i, filepath): 

    # 写入文件
    print("正在获取歌曲{}的第{}页评论！\n".format(songname, i))

    m = 1   # 记录第几条精彩评论
    data = {}   # 存储数据

    # 键在字典中则返回True, 否则返回False
    if 'hotComments' in content_json:

        # 遍历每一条热评
        for item in content_json['hotComments']:

            # 热评的用户
            user = item['user']

            # 热评的用户ID
            data['user_id'] = user['userId']

            # 热评的用户名
            data['user_name'] = user['nickname']

            # 热评ID
            data['comment_id'] = item['commentId']

            # 热评内容
            data['comment'] = item['content'].replace("\n"," ")
            
            # 热评时间
            data['time'] = item['timeStr']

            # 热评点赞数
            data['likecount'] = item['likedCount']

            # 评论为空，跳过
            if(data['comment'] == None):
                continue
                
            # 评论省份
            if item['ipLocation']['location'] == "":
                data['location'] = "null"
            else:
                data['location'] = item['ipLocation']['location']

            save_csv(filepath, data)

            m += 1


# 从网页url中提取普通评论
def comments(content_json, songname, i, filepath):

    print("正在获取歌曲{}的第{}页评论！\n".format(songname, i))
    
    # 全部评论
    j = 1
    data = {}
    for item in content_json['comments']:

        # 发表评论的用户
        user = item['user']

        # 发表评论的用户ID
        data['user_id'] = user['userId']

        # 发表评论的用户名
        data['user_name'] = user['nickname']

        # 发表评论ID
        data['comment_id'] = item['commentId']

        # 发表评论内容
        data['comment'] = item['content'].replace("\n"," ")
        
        # 发表评论时间
        data['time'] = item['timeStr']

        # 发表评论点赞数
        data['likecount'] = item['likedCount']

        # 发表评论为空，跳过
        if(data['comment'] == None):
            continue
            
        # 发表评论省份
        if item['ipLocation']['location'] == "":
            data['location'] = "null"
        else:
            data['location'] = item['ipLocation']['location']


        save_csv(filepath, data)

        j += 1


def get_song_comments(songname, songid, filepath):

    with open(filepath, 'a', encoding='utf-8') as file:
        file.write("{},{},{},{},{},{},{}\n".format("user_id", "user_name", "comment_id", "comment", "time", "likecount", "location"))

    print('开始爬取!')

    page = 0    # 第一页
    
    url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'

    # 获取第一页评论，json格式
    content_json = get(url)

    # 评论总数
    total = content_json['total']

    # 总页数
    pages = math.ceil(total / 20)

    print("总共有{}页{}条评论\n".format(pages, total))

    hotcomments(content_json, songname, page, filepath)
    comments(content_json, songname, page, filepath)

    # 开始获取歌曲的全部评论
    page = 1

    # 取50个页
    while page < 50:

        url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'
        content_json = get(url)

        # 从第二页开始获取评论
        comments(content_json, songname, page, filepath)
        page += 1
    
    print("爬取结束!")



if __name__ == "__main__":

    songname = "song_comments"
    filepath = f"data\{songname}.txt"
    get_song_comments(songname, 4944428, filepath)
