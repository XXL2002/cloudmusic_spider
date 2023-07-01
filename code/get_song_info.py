from utils import get


def get_song_info(songid):

    url = f'https://music.163.com/api/song/detail/?id={songid}&ids=[{songid}]'
    content_json = get(url)
    
    data = {}
    for song in content_json['songs']:
        
        # 获取歌曲名
        data['songname'] = song['name']
        print(data['songname'])

        # 获取歌手名(只获取第一个)
        data['songer'] = song['artists'][0]['name']
        print(data['songer'])

        # 获取所属专辑
        data['album'] = song['album']['name']
        print(data['album'])

        return data


if __name__ == "__main__":

    get_song_info(1959190717)