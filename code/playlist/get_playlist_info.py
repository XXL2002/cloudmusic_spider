# 爬取相关数据
import sys
sys.path.append("code")
from tools.struct import file_info_paths
from tools.file import save_csv
from tools.request import get
from tools.utils import list2str

def get_playlist_info(playlistid):
    '''
        获取指定歌单的基本信息
    '''
    print(f"开始爬取歌单{playlistid}的详细信息...")
    data = {}
    url = f'https://music.163.com/api/v1/playlist/detail?id={playlistid}'

    content_json = get(url)

    # 歌单ID
    data['playlist_id'] = content_json['playlist']['id']
    
    # 歌单名称
    data['playlist_name'] = content_json['playlist']['name']
    
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
    ids = [track['id'] for track in content_json['playlist']['trackIds']]
    ids = ids[0:5]
    data['trackIds'] = list2str(ids)

    save_csv(file_info_paths['playlist'], data)
    
    return ids
        


if __name__ == "__main__":

    get_playlist_info(19723756)    # 获取指定歌单的基本信息