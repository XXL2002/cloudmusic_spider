# coding='utf-8'
import sys
sys.path.append("code")

import math
from tools.request import get
from tools.comment import hotcomments, comments
from tools.sleep import sleep
from tools.progressBar import progress_bar


def get_song_comments(songid):
    '''
        获取歌曲评论
    '''
    filename = f"song_{songid}_comments"
    filepath = f"data/{filename}.txt"
    
    with open(filepath, 'a', encoding='utf-8') as file:
        file.write("{},{},{},{},{},{},{}\n".format("user_id", "user_name", "comment_id", "comment", "time", "likecount", "location"))

    print(f'开始爬取!==>{filename}')

    page = 0    # 第一页
    
    url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'

    # 获取第一页评论，json格式
    content_json = get(url)

    # 评论总数
    total = content_json['total']

    # 总页数
    pages = math.ceil(total / 20)

    print("总共有{}页{}条评论\n".format(pages, total))

    hotcomments(content_json, filepath)
    comments(content_json, filepath)

    # 开始获取歌曲的全部评论
    page = 1

    # 取50个页
    while page < pages and page < 75:
        if(page%5==0 or page==1):
            progress_bar(page,min(pages,75))
            
        url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'
        content_json = get(url)

        # 从第二页开始获取评论
        comments(content_json, filepath)
        page += 1

        sleep()
    
    print("爬取结束!")



if __name__ == "__main__":

    # songname = "song_comments"
    # filepath = f"data\{songname}.txt"
    get_song_comments(4944428)