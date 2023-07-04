# coding='utf-8'

import sys
sys.path.append("code")

import math
from tools.request import get
from tools.comment import hotcomments, comments
from tools.sleep import sleep
from tools.progressBar import progress_bar
from tools.struct import file_headers
from tools.file import add_header

def get_playlist_comments(playlistid):
    '''
        获取歌单的评论
    '''
    filename = f"playlist_{playlistid}"
    filepath = f"data\playlist_comments\{filename}.txt"

    add_header(filepath, file_headers['comment'])

    print(f'开始爬取!==>{filename}')

    page = 0    # 第一页
    
    url = f'https://music.163.com/api/v1/resource/comments/A_PL_0_{playlistid}?limit=20&offset={page}'

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

    # 爬取前75页评论
    while page < pages and page < 50:
        if(page%5==0 or page==1):
            progress_bar(page,min(pages,50))

        url = f'https://music.163.com/api/v1/resource/comments/A_PL_0_{playlistid}?limit=20&offset={page}'
        content_json = get(url)

        # 从第二页开始获取评论
        comments(content_json, filepath)
        page += 1
        
        sleep()
    
    print("爬取结束!")



if __name__ == "__main__":

    # filename = "playlist_comments"
    # filepath = f"data\{filename}.txt"
    get_playlist_comments(2105681544)
