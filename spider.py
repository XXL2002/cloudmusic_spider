# 爬取相关数据

import json
import time
from urllib import request
from bs4 import BeautifulSoup

# 城市字典
city_dic = {
    110000: '北京市',
    120000: '天津市',
    130000: '河北省',
    140000: '山西省',
    150000: '内蒙古自治区',
    210000: '辽宁省',
    220000: '吉林省',
    230000: '黑龙江省',
    310000: '上海市',
    320000: '江苏省',
    330000: '浙江省',
    340000: '安徽省',
    350000: '福建省',
    360000: '江西省',
    370000: '山东省',
    410000: '河南省',
    420000: '湖北省',
    430000: '湖南省',
    440000: '广东省',
    450000: '广西壮族自治区',
    460000: '海南省',
    500000: '重庆市',
    510000: '四川省',
    520000: '贵州省',
    530000: '云南省',
    540000: '西藏自治区',
    610000: '陕西省',
    620000: '甘肃省',
    630000: '青海省',
    640000: '宁夏回族自治区',
    650000: '新疆维吾尔自治区',
}

# 伪装请求头
headers = {
    'Host': 'music.163.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}


def get_comments(page):
    """
        获取评论信息
    """
    url = 'https://music.163.com/api/v1/resource/comments/R_SO_4_2004684052?limit=20&offset=' + str(page)   # 页面url
    req = request.Request(url, headers=headers)  #使用request封装url和头部信息
    response = request.urlopen(req)     # 请求页面

    # 将返回页面数据转为json格式
    content_json = json.loads(response.read().decode("utf-8"))

    with open('./test.json', 'w', encoding='utf-8') as file:
        json.dump(content_json, file)

    # 获取评论数据
    items = content_json['comments']
    for item in items:

        # 用户名
        user_name = item['user']['nickname'].replace(',', '，')
        print(user_name, end=" ")

        # 用户ID
        user_id = str(item['user']['userId'])
        print(user_id, end=" ")

        # 获取用户信息
        user_message = get_user(user_id)

        # 用户年龄
        user_age = str(user_message['age'])
        print(user_age, end=" ")

        # 用户性别
        user_gender = str(user_message['gender'])
        print(user_gender, end=" ")

        # 用户所在地区
        user_city = str(user_message['city'])
        print(user_city, end=" ")

        # # 个人介绍
        # user_introduce = user_message['sign'].strip().replace('\n', '').replace(',', '，')
        # # 评论内容
        # comment = item['content'].strip().replace('\n', '').replace(',', '，')
        # # 评论ID
        # comment_id = str(item['commentId'])
        # # 评论点赞数
        # praise = str(item['likedCount'])
        # # 评论时间
        # date = time.localtime(int(str(item['time'])[:10]))
        # date = time.strftime("%Y-%m-%d %H:%M:%S", date)
        # print(user_name, user_id, user_age, user_gender, user_city, user_introduce, comment, comment_id, praise, date)

        # with open('music_comments.csv', 'a', encoding='utf-8-sig') as f:
        #     f.write(user_name + ',' + user_id + ',' + user_age + ',' + user_gender + ',' + user_city + ',' + user_introduce + ',' + comment + ',' + comment_id + ',' + praise + ',' + date + '\n')
        # f.close()

        print()


def get_user(user_id):
    """
    获取用户注册时间
    """
    data = {}
    url = 'https://music.163.com/api/v1/user/detail/' + str(user_id)

    req = request.Request(url, headers=headers)  #使用request封装url和头部信息
    response = request.urlopen(req)     # 请求页面

    # 将页面数据转为json格式
    content_json = json.loads(response.read().decode("utf-8"))

    # 成功返回
    if content_json['code'] == 200:

        # 性别
        data['gender'] = content_json['profile']['gender']

        # 年龄
        if int(content_json['profile']['birthday']) < 0:
            data['age'] = 0
        else:
            data['age'] = (2018 - 1970) - (int(content_json['profile']['birthday']) // (1000 * 365 * 24 * 3600))
        
        if int(data['age']) < 0:
            data['age'] = 0

        # 省份
        data['province'] = city_dic[content_json['profile']['province']]

        # 个人介绍
        data['sign'] = content_json['profile']['signature']

    else:

        data['gender'] = '无'
        data['age'] = '无'
        data['city'] = '无'
        data['sign'] = '无'

    return data


# get_comments(0)
print(get_user(341767152))