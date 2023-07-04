# coding='utf-8'

import sys
sys.path.append("code")

from tools.request import get
import re


def get_song_lyric(songid):
    '''
        根据歌曲ID获取歌词,每句歌词之间以逗号间隔
    '''

    url = f'https://music.163.com/api/song/lyric?id={songid}&lv=1&kv=1&tv=-1'
    content_json = get(url)

    lyric = content_json['lrc']['lyric'].replace("\n", "")
    pattern1 = r"\[[^\]]*\]"
    lyric = re.sub(pattern1, " ", lyric).lstrip()
    pattern2 = r"作词\s*:\s*\w+\s*"
    lyric = re.sub(pattern2, "", lyric)
    pattern3 = r"作曲\s*:\s*\w+\s*"
    lyric = re.sub(pattern3, "", lyric)
    pattern4 = r'制作人\s*:\s*\w+\s*'
    lyric = re.sub(pattern4, "", lyric)
    pattern4 = r'编曲\s*:\s*\w+\s*'
    lyric = re.sub(pattern4, "", lyric)

    return lyric


if __name__ == "__main__":

    lyric = get_song_lyric(2057534370)
    print(lyric)