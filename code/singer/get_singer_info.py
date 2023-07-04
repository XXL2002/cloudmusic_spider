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

    # 获取歌手用户id
    data['singer_id'] = content_json['artist']['accountId']

    # 获取歌手名称
    data['singer_name'] = content_json['artist']['name']
    
    url = 'https://music.163.com/api/v1/user/detail/' + str(data['singer_id'])
    content_json1 = get(url)

    # 获取歌手粉丝
    data['fans'] = content_json1['profile']['followeds']

    # 获取歌手热门歌曲id
    hotSongs = [hotSong['id'] for hotSong in content_json['hotSongs']]
    data['hotsongs'] = list2str(hotSongs)

    save_csv(file_info_paths['singer'], data)

    # return

if __name__=="__main__":

    get_singer_info(10559)


