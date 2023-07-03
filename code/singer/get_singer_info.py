# coding='utf-8'

import sys
sys.path.append("code")

from tools.request import get


def get_singer_info(singer_id):

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

    return data

if __name__=="__main__":

    data = get_singer_info(10559)
    print(data)


