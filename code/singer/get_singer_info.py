# coding='utf-8'

import sys
sys.path.append("code")
from song.get_song_info import get_song_info
from tools.file import save_csv
from tools.request import get
from tools.struct import file_info_paths
from tools.utils import list2str

def get_singer_info(singer_id):
    '''
    获取指定歌手的基本信息
    '''
    if singer_id == -1:
        return
    
    url = f'http://music.163.com/api/artist/{singer_id}'
    data = {}
    content_json = get(url)
    
    if content_json is None:
        return

    # 歌手ID
    data['singer_id'] = singer_id
    
    # 获取歌手名称
    data['singer_name'] = content_json['artist']['name']
    
    # accountId存在时有歌手个人主页
    if 'accountId' in content_json['artist']:

        # 获取歌手用户id
        data['accountId'] = content_json['artist']['accountId']

        url = 'https://music.163.com/api/v1/user/detail/' + str(data['accountId'])
        content_json1 = get(url)

        # 获取歌手粉丝
        data['fans'] = content_json1['profile']['followeds']


    else:

        data['accountId'] = 'null'
        data['fans'] = 'null'


    # 获取歌手热门歌曲id
    hotSongs = [hotSong['id'] for hotSong in content_json['hotSongs']]
    data['hotsongs'] = list2str(hotSongs)

    save_csv(file_info_paths['singer'], data)
    
    # 爬取该歌手热门歌曲的基本信息
    for song in hotSongs:
        get_song_info(song)

    return


if __name__=="__main__":

    data = get_singer_info(3684)
    print(data)


