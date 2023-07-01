import time
import datetime
import openpyxl
from Crypto.Cipher import AES
import base64
import codecs
import random
import math

# 伪造请求头
headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'WM_TID=36fj4OhQ7NdU9DhsEbdKFbVmy9tNk1KM; _iuqxldmzr_=32; _ntes_nnid=26fc3120577a92f179a3743269d8d0d9,1536048184013; _ntes_nuid=26fc3120577a92f179a3743269d8d0d9; __utmc=94650624; __utmz=94650624.1536199016.26.8.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); WM_NI=2Uy%2FbtqzhAuF6WR544z5u96yPa%2BfNHlrtTBCGhkg7oAHeZje7SJiXAoA5YNCbyP6gcJ5NYTs5IAJHQBjiFt561sfsS5Xg%2BvZx1OW9mPzJ49pU7Voono9gXq9H0RpP5HTclE%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed5cb8085b2ab83ee7b87ac8c87cb60f78da2dac5439b9ca4b1d621f3e900b4b82af0fea7c3b92af28bb7d0e180b3a6a8a2f84ef6899ed6b740baebbbdab57394bfe587cd44b0aebcb5c14985b8a588b6658398abbbe96ff58d868adb4bad9ffbbacd49a2a7a0d7e6698aeb82bad779f7978fabcb5b82b6a7a7f73ff6efbd87f259f788a9ccf552bcef81b8bc6794a686d5bc7c97e99a90ee66ade7a9b9f4338cf09e91d33f8c8cad8dc837e2a3; JSESSIONID-WYYY=G%5CSvabx1X1F0JTg8HK5Z%2BIATVQdgwh77oo%2BDOXuG2CpwvoKPnNTKOGH91AkCHVdm0t6XKQEEnAFP%2BQ35cF49Y%2BAviwQKVN04%2B6ZbeKc2tNOeeC5vfTZ4Cme%2BwZVk7zGkwHJbfjgp1J9Y30o1fMKHOE5rxyhwQw%2B%5CDH6Md%5CpJZAAh2xkZ%3A1536204296617; __utma=94650624.1052021654.1536048185.1536199016.1536203113.27; __utmb=94650624.12.10.1536203113',
            'Host': 'music.163.com',
            'Referer': 'http://music.163.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/66.0.3359.181 Safari/537.36'}


# 城市字典
city_dic = {
    0: '未知',
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


# 将json格式的值列表转换为逗号分割的字符串
def json2str(data):

    return ','.join([str(i) for i in list(data.values())])


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


# 根据生日时间戳(ms)，计算出年龄
def user_age(given_timestamp):

    current_timestamp = int(time.time() * 1000)  # 当前时间戳（以毫秒为单位）

    given_datetime = datetime.datetime.fromtimestamp(given_timestamp / 1000)
    current_datetime = datetime.datetime.fromtimestamp(current_timestamp / 1000)

    age_timedelta = current_datetime - given_datetime

    return age_timedelta.days // 365


# 将时间戳(ms)转换为日期与时间
# def timestamp2date(timestamp):
#     str = str(datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S"))
#     date, time = str.split(" ")

#     return date, time


# 将json格式值数据写入csv文件
def save_csv(path, data):
    
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json2str(data) + "\n")
        
    f.close()
    


def create_xlsx(path, header):

    # 创建工作簿
    workbook = openpyxl.Workbook()

    # 获取表
    sheet = workbook.active

    # 设置表头
    sheet.append(header)
    
    # 保存文件
    workbook.save(path)

    return sheet



