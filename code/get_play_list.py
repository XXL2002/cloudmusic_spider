# 爬取相关数据

import json
from urllib import request

import requests
from utils import headers


def get_play_list(play_list_id):
    """
    获取指定歌单的基本信息
    """
    data = {}
    url = 'https://music.163.com/api/v1//playlist/detail?id=' + str(play_list_id)

    text = requests.get(url, headers=headers)
    content_json = text.json()
    
    # 成功返回
    try:
        if content_json['code'] == 200:
            # 歌单ID
            data['id'] = content_json['playlist']['id']
            
            # 歌单名称
            data['name'] = content_json['playlist']['name']
            
            # 播放量
            data['playCount'] = content_json['playlist']['playCount']
            
            # 收藏数
            data['subscribedCount'] = content_json['playlist']['subscribedCount']
            
            # 歌单描述
            data['description'] = content_json['playlist']['description']

            # 歌单标签
            data['tags'] = content_json['playlist']['tags']
            
            # 创建者ID
            data['creator'] = content_json['playlist']['creator']['userId']
            
            # 歌曲id列表
            data['tracks'] = ""
            for i in range(0,len(content_json['playlist']['tracks'])):
                data['tracks'] += str(content_json['playlist']['tracks'][i]['id'])+' '
            
            print(data)
            # 关闭连接
            text.close()
            return data
        
    except:

        print("爬取失败!")
        


if __name__ == "__main__":

    get_play_list(2105681544)    # 获取指定歌单的基本信息