import sys
sys.path.append("code")

import requests
from tools.struct import headers


# get请求，返回json数据
def get(url):

    response = requests.get(url, headers=headers)
    content_json = response.json()

    try:
        if content_json['code'] == 200:

            response.close()
            return content_json
        
    except:

        print("爬取失败!")
    

# post请求
def post(url, data):

    response = requests.post(url, headers=headers, data=data)
    content_json = response.json()

    try:
        if content_json['code'] == 200:

            response.close()
            return content_json
        
    except:

        print("爬取失败!")
