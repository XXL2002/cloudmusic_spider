# coding='utf-8'
import sys
sys.path.append("code")

import math
from tools.request import get
from tools.comment import hotcomments, comments
from tools.sleep import sleep
from tools.progressBar import progress_bar
from tools.file import add_header
from tools.struct import file_headers


def get_song_comments(songid):
    '''
        获取歌曲评论
    '''

    filename = f"song_{songid}"
    filepath = f"data/song_comments/{filename}.txt"
    
    add_header(filepath, file_headers['comment'])

    print(f'\t\t开始爬取单曲评论!==>{filename}')

    page = 0    # 第一页
    
    url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'

    # 获取第一页评论，json格式
    content_json = get(url)

    # 评论总数
    total = content_json['total']

    # 总页数
    pages = math.ceil(total / 20)

    print("\t\t总共有{}页{}条评论\n".format(pages, total))

    users = []
    users += hotcomments(content_json, filepath)
    users += comments(content_json, filepath)

    # 开始获取歌曲的全部评论
    page = 1

    # 取5个页
    while page < pages and page < 5:
        if(page == 1):
            progress_bar(page,min(pages,5))
            
        url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'
        content_json = get(url)

        # 从第二页开始获取评论
        users += comments(content_json, filepath)
        page += 1

        if ((page+1) % 5 == 0 or page ==min(pages,5)-1):
            progress_bar(page+1,min(pages,5))
        sleep()
    
    print("爬取结束!")
    return users



if __name__ == "__main__":

    # songname = "song_comments"
    # filepath = f"data\{songname}.txt"
    get_song_comments(4944428)
