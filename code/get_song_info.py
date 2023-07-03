from utils import get
from get_song_lyric import get_song_lyric

# 获取歌曲基本信息
def get_song_info(songid):

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

        return data


if __name__ == "__main__":

    data = get_song_info(1959190717)
    print(data)