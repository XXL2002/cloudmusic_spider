#! /usr/bin/env python
# coding='utf-8'

import requests
import math
import random
from Crypto.Cipher import AES
import codecs
import base64
from utils import headers

# 获取热评
def hotcomments(html, songname, i, filepath): 

    # 写入文件
    print("正在获取歌曲{}的第{}页评论！\n".format(songname, i))

    # 记录第几条精彩评论
    m = 1

    # 键在字典中则返回True, 否则返回False
    if 'hotComments' in html:

        # 遍历每一条热评
        for item in html['hotComments']:

            # 热评的用户
            user = item['user']

            # 热评的用户ID
            user_id = user['userId']

            # 热评的用户名
            user_name = user['nickname']

            # 热评ID
            comment_id = item['commentId']

            # 热评内容
            comment = item['content'].replace("\n"," ")
            
            # 热评时间
            time = item['timeStr']

            # 热评点赞数
            likecount = item['likedCount']

            # 评论为空，跳过
            if(comment == None):
                continue
                
            # 评论省份
            if item['ipLocation']['location'] == "":
                location = "null"
            else:
                location = item['ipLocation']['location']

            # print("热门评论{}: {} ".format(m, comment))

            with open(filepath, 'a', encoding='utf-8') as f:

                f.write("{},{},{},{},{},{},{}\n".format(user_id, user_name, comment_id, comment, time, likecount, location))
                
                # 回复评论
                # if len(item['beReplied']) != 0:

                #     for reply in item['beReplied']:

                #         # 发表回复评论的用户
                #         replyuser = reply['user']

                #         # 发表回复评论的用户ID
                #         replyuser_id = replyuser['userId']

                #         # 发表回复评论的用户名
                #         replyuser_name = replyuser['nickname']

                #         print(reply)
                #         # 回复评论ID
                #         replycomment_id = reply['beRepliedCommentId']

                #         # 回复评论内容
                #         replycomment = reply['content'].replace("\n"," ")

                #         # 回复评论为空，跳过
                #         if(replycomment == None):
                #             continue
                            
                #         # 回复评论省份
                #         replylocation = reply['ipLocation']['location']

                #         if(replycomment == None):
                #             continue

                #         print("{}".format(reply['content']))
                #         f.write("{},{},{},{},{},{},{}\n".format( replyuser_id,  replyuser_name,  replycomment_id,  replycomment,  replytime,  replylikecount,  replylocation))
            
            m += 1
            f.close()


# 获取普通评论
def comments(html, songname, i, filepath):

    print("正在获取歌曲{}的第{}页评论！\n".format(songname, i))
    
    # 全部评论
    j = 1
    for item in html['comments']:

        # 发表评论的用户
        user = item['user']

        # 发表评论的用户ID
        user_id = user['userId']

        # 发表评论的用户名
        user_name = user['nickname']

        # 发表评论ID
        comment_id = item['commentId']

        # 发表评论内容
        comment = item['content'].replace("\n"," ")
        
        # 发表评论时间
        time = item['timeStr']

        # 发表评论点赞数
        likecount = item['likedCount']

        # 发表评论为空，跳过
        if(comment == None):
            continue
            
        # 发表评论省份
        if item['ipLocation']['location'] == "":
            location = "null"
        else:
            location = item['ipLocation']['location']

        # print("发表评论{}: {} ".format(j, comment))

        with open(filepath, 'a', encoding='utf-8') as f:

            f.write("{},{},{},{},{},{},{}\n".format(user_id, user_name, comment_id, comment, time, likecount, location))
            
            # 回复评论
            # if len(item['beReplied']) != 0:

            #     for reply in item['beReplied']:

            #         # 提取发表回复评论的用户名
            #         if(reply['content']==None):
            #             continue

            #         replyuser = reply['user']
            #         print("{}".format(reply['content']))
            #         f.write("   reply: {} {} {}\n".format(reply['content'].replace("\n"," "), reply['ipLocation']['location'], reply['ipLocation']['userId']))
        
        j += 1
        f.close()


# 构造函数获取歌手信息
def get_comments_json(url, data):

    try:
        
        r = requests.post(url, headers=headers, data=data)
        r.encoding = "utf-8"

        # 成功返回，转换为json格式
        if r.status_code == 200:
            return r.json()
        
    except:

        print("爬取失败!")


# 生成指定长度的随机字符串
def generate_random_strs(length):

    # 生成随机字符串的字符池(大小写字母+数字)
    char_pool = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    # 循环次数
    i = 0

    # 初始化随机字符串
    random_str  = ""
    while i < length:

        # random.random()生成0-1随机数，* 指定长度并向下取整，得到字符池里的随机一个下标
        e = math.floor(random.random() * len(char_pool))

        # 从字符池中取出并加上
        random_str += list(char_pool)[e]

        i = i + 1

    return random_str


# AES加密
def AESencrypt(msg, key):

    # 如果不是16的倍数则进行填充，计算需要填充的位数
    padding = 16 - len(msg) % 16

    # 这里使用padding对应的单字符进行填充
    msg = msg + padding * chr(padding)

    # 用来加密或者解密的初始向量(必须是16位)
    iv = '0102030405060708'
    
    key=key.encode('utf-8')
    iv=iv.encode('utf-8')
    msg=msg.encode('utf-8')

    cipher = AES.new(key, AES.MODE_CBC, iv)

    # 加密后得到的是bytes类型的数据
    encryptedbytes = cipher.encrypt(msg)

    # 使用Base64进行编码,返回byte字符串
    encodestrs = base64.b64encode(encryptedbytes)

    # 对byte字符串按utf-8进行解码
    enctext = encodestrs.decode('utf-8')

    return enctext


# RSA加密
def RSAencrypt(randomstrs, key, f):

    # 随机字符串逆序排列
    string = randomstrs[::-1]

    # 将随机字符串转换成byte类型数据
    text = bytes(string, 'utf-8')

    seckey = int(codecs.encode(text, encoding='hex'), 16)**int(key, 16) % int(f, 16)
    
    return format(seckey, 'x').zfill(256)


# 获取参数:两次AES加密生成encText，再进行RSA加密生成encSecKey
def get_params(page):
    
    # msg也可以写成msg = {"offset":"页面偏移量=(页数-1) *　20", "limit":"20"},offset和limit这两个参数必须有(js)
    # limit最大值为100,当设为100时,获取第二页时,默认前一页是20个评论,也就是说第二页最新评论有80个,有20个是第一页显示的
    # msg = '{"rid":"R_SO_4_1302938992","offset":"0","total":"True","limit":"100","csrf_token":""}'
    
    # 偏移量用于确定从哪个位置开始获取评论数据
    offset = (page-1) * 20

    # 生成一个包含评论请求参数的JSON字符串
    # offset--偏移量，total--是否包含总评论数，limit--每页的评论数量，csrf_token--一个可选的CSRF令牌
    # offset和limit是必选参数,其他参数是可选的,其他参数不影响data数据的生成
    msg = '{"offset":' + str(offset) + ',"total":"True","limit":"20","csrf_token":""}'
    key = '0CoJUm6Qyw8W8jud'    # 密钥用于加密评论请求参数
    enctext = AESencrypt(msg, key)  # 对评论请求参数msg进行加密，加密的结果存储在enctext变量中

    # 生成长度为16的随机字符串
    i = generate_random_strs(16)

    # 两次AES加密之后得到encText
    encText = AESencrypt(enctext, i)

    f = '00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
    e = '010001'

    # RSA加密之后得到encSecKey的值
    encSecKey = RSAencrypt(i, e, f)

    return encText, encSecKey


def get_song_comments(songname, songid, filepath):

    with open(filepath, 'a', encoding='utf-8') as file:
        file.write("{},{},{},{},{},{},{}\n".format("user_id", "user_name", "comment_id", "comment", "time", "likecount", "location"))

    print('开始爬取!')

    page = 1    # 第一页
    params, encSecKey = get_params(page)    # 评论请求参数
    url = 'https://music.163.com/weapi/v1/resource/comments/R_SO_4_' + str(songid) + '?csrf_token='
    data = {'params': params, 'encSecKey': encSecKey}
    
    # 获取第一页评论，json格式
    html = get_comments_json(url, data)

    # 评论总数
    total = html['total']

    # 总页数
    pages = math.ceil(total / 20)

    print("总共有{}页{}条评论\n".format(pages, total))

    hotcomments(html, songname, page, filepath)
    comments(html, songname, page, filepath)

    # 开始获取歌曲的全部评论
    page = 2

    # 取50个页
    while page <= 50:

        params, encSecKey = get_params(page)
        data = {'params': params, 'encSecKey': encSecKey}
        html = get_comments_json(url, data)

        # 从第二页开始获取评论
        comments(html, songname, page, filepath)
        page += 1
    
    print("爬取结束!")



if __name__ == "__main__":

    songname = "song_comments"
    filepath = f"data\{songname}.txt"
    get_song_comments(songname, 4944428, filepath)
