# coding='utf-8'

import sys
sys.path.append("code")

from tools.struct import city_dic
from tools.utils import user_age
from tools.request import get
from tools.struct import file_info_paths
from tools.file import save_csv

def get_user_info(user_id):
    '''
        获取用户基本信息
    '''
    
    data = {}
    url = f'https://music.163.com/api/v1/user/detail/{user_id}'

    content_json = get(url)

    # 用户名
    data['nickname'] = content_json['profile']['nickname']

    # 性别
    if content_json['profile']['gender'] == 1:
        data['gender'] = '男'
    elif content_json['profile']['gender'] == 2:
        data['gender'] = '女'
    else:
        data['gender'] = '未知'

    # 年龄
    if content_json['profile']['birthday'] < 0:     # 时间戳小于0，该用户未填年龄
        data['age'] = -1
    else: 
        data['age'] = user_age(content_json['profile']['birthday'])

    # 省份
    data['province'] = city_dic[content_json['profile']['province']]

    # 个人介绍
    if content_json['profile']['signature'] == "":
        data['signature'] = "无"
    else:
        data['signature'] = content_json['profile']['signature'].replace("\n","").replace("\u200b", "")

    save_csv(file_info_paths['user'], data)

    return data
        


if __name__ == "__main__":

    data = get_user_info(37132109)    # 获取指定用户的基本信息
    print(data)