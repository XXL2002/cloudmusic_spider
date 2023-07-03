# 爬取相关数据
from utils import get


def get_play_list(play_list_id):
    """
    获取指定歌单的基本信息
    """
    data = {}
    url = f'https://music.163.com/api/v1/playlist/detail?id={play_list_id}'

    content_json = get(url)

    # 歌单ID
    data['id'] = content_json['playlist']['id']
    
    # 歌单名称
    data['name'] = content_json['playlist']['name']
    
    # 播放量
    data['playCount'] = content_json['playlist']['playCount']
    
    # 收藏数
    data['subscribedCount'] = content_json['playlist']['subscribedCount']
    
    # 歌单描述
    data['description'] = content_json['playlist']['description'].replace("\n", "")

    # 歌单标签
    data['tags'] = content_json['playlist']['tags']
    
    # 创建者ID
    data['creator'] = content_json['playlist']['creator']['userId']
    
    # 歌曲id列表
    data['tracks'] = ' '.join([str(track['id']) for track in content_json['playlist']['tracks']])

    return data
        


if __name__ == "__main__":

    data = get_play_list(2105681544)    # 获取指定歌单的基本信息
    print(data)