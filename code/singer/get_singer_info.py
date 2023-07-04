# coding='utf-8'

import sys
sys.path.append("code")
from tools.file import save_csv
from tools.request import get


def get_singer_info(singer_id):
    '''
    获取指定歌手的基本信息
    '''
    filename = f"singer_info"
    filepath = f"data/{filename}.txt"
    
    url = f'http://music.163.com/api/artist/{singer_id}'
    data = {}
    content_json = get(url)

    # 获取歌手名称
    data['name'] = content_json['artist']['name']

    # 获取歌手用户id
    data['user_id'] = content_json['artist']['accountId']
    
    url = 'https://music.163.com/api/v1/user/detail/' + str(data['user_id'])
    content_json1 = get(url)

    # 获取歌手粉丝
    data['fans'] = content_json1['profile']['followeds']

    # 获取歌手热门歌曲id
    data['hotsongs'] = [hotSong['id'] for hotSong in content_json['hotSongs']]

    save_csv(filepath, data)

    return

if __name__=="__main__":

    get_singer_info(10559)


