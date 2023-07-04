# coding='utf-8'

import sys
sys.path.append("code")
from tools.file import save_csv
from tools.request import get
from song.get_song_lyric import get_song_lyric


def get_song_info(songid):
    '''
        获取歌曲基本信息
    '''

    filename = f"song_info"
    filepath = f"data/{filename}.txt"
    
    # with open(filepath, 'a', encoding='utf-8') as file:
    #     file.write("{},{},{},{}\n".format("songname", "songer", "album", "lyric"))
    
    url = f'https://music.163.com/api/song/detail/?id={songid}&ids=[{songid}]'
    content_json = get(url)
    
    data = {}
    for song in content_json['songs']:
        
        # 获取歌曲名
        data['songname'] = song['name']

        # 获取歌手名(只获取第一个)
        data['songer'] = song['artists'][0]['name']

        # 获取所属专辑
        data['album'] = song['album']['name']

        # 获取歌词
        data['lyric'] = get_song_lyric(songid)

        save_csv(filepath, data)
        
        return


if __name__ == "__main__":

    get_song_info(1959190717)