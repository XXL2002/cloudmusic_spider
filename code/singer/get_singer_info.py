# coding='utf-8'

import sys
sys.path.append("code")
from tools.file import save_csv
from tools.request import get
from tools.struct import file_info_paths
from tools.utils import list2str

def get_singer_info(singer_id):
    '''
    获取指定歌手的基本信息
    '''
    
    url = f'http://music.163.com/api/artist/{singer_id}'
    data = {}
    content_json = get(url)

    # 歌手ID
    data['singer_id'] = singer_id
    
    # accountId存在时有歌手个人主页
    if 'accountId' in content_json['artist']:

        # 获取歌手用户id
        data['accountId'] = content_json['artist']['accountId']

        url = 'https://music.163.com/api/v1/user/detail/' + str(data['accountId'])
        content_json1 = get(url)

        # 获取歌手粉丝
        data['fans'] = content_json1['profile']['followeds']


    else:

        data['accountId'] = 0
        data['fans'] = 0


    # 获取歌手名称
    data['singer_name'] = content_json['artist']['name']


    # 获取歌手热门歌曲id
    hotSongs = [hotSong['id'] for hotSong in content_json['hotSongs']]
    data['hotsongs'] = list2str(hotSongs)

    save_csv(file_info_paths['singer'], data)

    return data


if __name__=="__main__":

    data = get_singer_info(10559)
    print(data)


