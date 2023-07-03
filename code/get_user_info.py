# 爬取相关数据

from utils import city_dic, json2str, user_age, get


def get_user(user_id):
    
    """
    根据用户ID获取用户基本信息
    """

    data = {}
    url = f'https://music.163.com/api/v1/user/detail/{user_id}'

    content_json = get(url)

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


    return data
        


if __name__ == "__main__":

    data = get_user(507974556)    # 获取指定用户的基本信息
    print(json2str(data))