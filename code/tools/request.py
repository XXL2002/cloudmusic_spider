import sys
sys.path.append("code")

import requests
from tools.struct import get_header


# get请求，返回json数据
def get(url):

    response = requests.get(url, headers=get_header())
    content_json = response.json()

    try:
        if content_json['code'] == 200:

            response.close()
            return content_json
        
    except:

        print("爬取失败!")
    

# post请求
def post(url, data):

    response = requests.post(url, headers=get_header(), data=data)
    content_json = response.json()

    try:
        if content_json['code'] == 200:

            response.close()
            return content_json
        
    except:

        print("爬取失败!")