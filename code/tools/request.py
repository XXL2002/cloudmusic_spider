import sys
sys.path.append("code")

import requests
from tools.struct import get_header


# get请求，返回json数据
def get(url):

    try:
        response = requests.get(url, headers=get_header())
        content_json = response.json()

    
        if content_json['code'] == 200:

            response.close()
            return content_json
        
    except:

        print("爬取失败!")
    

# post请求
def post(url, data):

    try:
        response = requests.post(url, headers=get_header(), data=data)
        content_json = response.json()

        if content_json['code'] == 200:

            response.close()
            return content_json
        
    except:

        print("爬取失败!")
