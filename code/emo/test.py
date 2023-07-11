# -*- encoding:utf-8 -*-
from snownlp import *
# from pyspark import SparkConf, SparkContext
# from pyhdfs import HdfsClient
import re
import os
os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/envs/pyspark_3.6/bin/python"
# import sys 
# sys.path.append('C:\\Users\\username\\anaconda3\\envs\\automodel_env\\Lib\\site-packages')
# sys.path = list(set(sys.path))
# sys.path


# 计算一首歌的歌词emo指数
def score_lyr(lyric):

    if lyric == 'null':     # 没有歌词
        
        return -1
    
    else:

        depart = lyric.split(' ')   # 获取歌词列表
        score_list = [SnowNLP(item).sentiments for item in depart if item != '']
        score = sum(score_list) / len(score_list)

        return score


# 计算所有歌曲的歌词emo指数
def score_lyrics(sc):

    # 获取歌曲(id-歌词emo指数)
    lyric_emo_list = sc.textFile('hdfs://stu:9000/basic_data/info/song_info.txt') \
            .map(lambda line: line.split(' @#$#@ ')) \
            .map(lambda list: [list[0], list[5]]) \
            .map(lambda list: (list[0], score_lyr(list[1]))) \
            .filter(lambda x: x[1] != -1) \
            .collect()
    
    return lyric_emo_list


# 计算单个评论文件的emo指数
def score_comment(rdd):

    # 获取评论的（点赞数-emo指数）列表
    comment_emo_list = rdd.map(lambda line: line.split(" @#$#@ ")) \
                            .map(lambda list: [list[3], list[4]]) \
                            .map(lambda x: (x[0], SnowNLP(x[1]).sentiments)) \
                            .collect()
    
    sum_likecount = sum(item[0] for item in comment_emo_list) + len(comment_emo_list)   # 点赞总数，每条评论点赞数加1，方式出现0权重
    score = sum((item[0] + 1) / sum_likecount * item[1] for item in comment_emo_list)   # 一首歌的emo指数
    
    return score


def score_comments(sc, client):

    # 获取所有歌单评论文件的rdd
    playlist_files = client.listdir('/basic_data/playlist_comments/')
    playlist_rdds = [sc.textFile('hdfs://stu:9000/basic_data/playlist_comments/' + file) for file in playlist_files]
    
    # 获取所有歌曲评论文件的emo指数
    song_files = client.listdir('/basic_data/song_comments/')
    song_rdds = [sc.textFile('hdfs://stu:9000/basic_data/song_comments/' + file) for file in song_files]

    # 存放歌单id对应emo指数
    playlist_emo_dict = {}

    # 存放歌曲id对应的emo指数
    songs_emo_dict = {}

    for file, rdd in zip(playlist_files, playlist_rdds):
        id = re.search(r'\d+', file).group(0)
        score = score_comment(rdd)
        playlist_emo_dict[id] = score

    for file, rdd in zip(song_files, song_rdds):
        id = re.search(r'\d+', file).group(0)
        score = score_comment(rdd)
        songs_emo_dict[id] = score

    return playlist_emo_dict, songs_emo_dict


# 计算单个歌曲的emo指数
def score_song(list, lyric_emo_list, songs_emo_dict):

    song_id = list[0]   # 歌曲id
    if lyric_emo_list.has_key(song_id):     # 有歌词
        score = 0.3 * lyric_emo_list[song_id] + 0.7 * songs_emo_dict[id]
    else:
        score = songs_emo_dict[id]

    # if len(list) == 8:      # 有emo指数
    #     list[7] = score
    # else:
    #     list.append(score)

    list.append(score)


# 计算歌曲的emo指数
def score_songs(sc, client):
    
    # 歌曲id-emo指数
    lyric_emo_list = score_lyrics(sc)

    # 歌单id-emo指数，歌曲id-emo指数
    playlist_emo_dict, songs_emo_dict = score_comments(sc, client)

    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/song_info.txt')
    
    rdd.map(lambda line: line.split(' @#$#@ ')) \
        .map(lambda list: score_song(list, lyric_emo_list, songs_emo_dict)) \
        .map(lambda list: ' @#$#@ '.join(list)) \
        .saveAsTextFile('hdfs://stu:9000/basic_data/info/test_song_info.txt')


# 计算单个用户的emo指数
def score_user(list, song_emo_dict):

    if signature != 'null' and signature != '':   # 有个性签名且不为空
        signature = list[5]

        if list[8] != 'null':   # 有近一周的听歌记录（一定有全部听歌排行）
            all_rank = list[7].split(' ')   # 全部听歌排行
            week_rank = list[8].split(' ')  # 近一周听歌排行

            all_list = [song_emo_dict(song_id) for song_id in all_rank if song_emo_dict.has_key(song_id)]
            all_score = sum(all_list) / len(all_list)

            week_list = [song_emo_dict(song_id) for song_id in week_rank if song_emo_dict.has_key(song_id)]
            week_score = sum(week_list) / len(week_list)

            list.append(0.2 * SnowNLP(signature).sentiments + 0.3 * all_score + 0.5 * week_score)
        
        else:

            list.append(SnowNLP(signature).sentiments)
        
    else:

        if list[8] != 'null':   # 有近一周的听歌记录（一定有全部听歌排行）
            all_rank = list[7].split(' ')   # 全部听歌排行
            week_rank = list[8].split(' ')  # 近一周听歌排行

            all_list = [song_emo_dict(song_id) for song_id in all_rank if song_emo_dict.has_key(song_id)]
            all_score = sum(all_list) / len(all_list)

            week_list = [song_emo_dict(song_id) for song_id in week_rank if song_emo_dict.has_key(song_id)]
            week_score = sum(week_list) / len(week_list)

            list.append(0.3 * all_score + 0.7 * week_score)
        
        else:

            list.append(-1)



# 计算用户的emo指数
def score_users(sc, song_emo_dict):

    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/user_info.txt')

    rdd.map(lambda list: score_user(list, song_emo_dict)) \
        .filter(lambda list: list[11] != -1) \
        .map(lambda list: ' @#$#@ '.join(list)) \
        .saveAsTextFile('hdfs://stu:9000/basic_data/info/test_user_info.txt')


# 计算歌手的emo指数
def score_singer(list, song_emo_dict):

    hotSongs = list[4].split(' ')   # 歌手的热门歌曲
    score_list = [song_emo_dict(song_id) for song_id in hotSongs if song_emo_dict.has_key(song_id)]
    score = sum(score_list) / len(score_list)
    
    list.append(score)


# 计算歌手的emo指数
def score_singers(sc, songs_emo_dict):

    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/singer_info.txt')

    rdd.map(lambda list: score_singer(list, songs_emo_dict)) \
        .map(lambda list: ' @#$#@ '.join(list)) \
        .saveAsTextFile('hdfs://stu:9000/basic_data/info/test_singer_info.txt')


# # 计算歌单的emo指数
# def score_playlists(sc, ):

#     rdd = sc.textFile('hdfs://stu:9000/basic_data/info/playlist_info.txt')

#     rdd.map(lambda list: score_singer(list, songs_emo_dict)) \
#         .map(lambda list: ' @#$#@ '.join(list)) \
#         .saveAsTextFile('hdfs://stu:9000/basic_data/info/test_singer_info.txt')
        


# def pys():
#     conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
#     sc = SparkContext(conf=conf)
#     lines=sc.textFile("hdfs://fwt:9000/song_info.txt")
#     # lines.distinct()\
#     lines_total = lines.map(lambda line: [line.split(" @#$#@ ")[0], line.split(" @#$#@ ")[5]]) \
#         .map(lambda x: score_total(str(x[0]), str(x[1])))\
#         .collect()
    
#     return lines_total


if __name__=="__main__":

    # conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    # sc = SparkContext(conf=conf)

    # client = HdfsClient(hosts='stu:50070', user_name='root')

    # playlist_emo_dict, songs_emo_dict = score_comments(sc, client)

    lyric = '等天崩地裂的那一瞬间 待坍塌破碎了睁开双眼 无处安放灵魂只能降落 若灵魂相结在天地之间 看山盟海誓引一场惊觉 没有你的世界我无力承受 怪我的心太炽热 初见的树是第几棵 树下彩蝶憩息着 你靠在我怀里说人生几何 这些画面我都记得 可微风在奏离歌 言不尽悲欢离合 窈窕淑女君子好逑 可我在漂流 两个人的逍遥游 没熬到船到桥头 像对准我的矛头 如今我借酒浇愁 若时光能够倒流 能否换个方式相拥  But I 拔情诀爱的最后 指尖缠绕的温柔 化作一把锋利剑 刺向了我 诀爱 模糊光阴距离 连时间都暂停 全都只是因为你为了你 确定从一开始就确定 你是心中唯一 说再见来不及 开不了口的惋惜 来不及 如今天各一方 就快熄灭的烛火飞蛾不会扑向 往胸口射一枪 将你爱的模样 全部统统都埋葬 冲出幻象 梦也该醒醒了吧 趁天没亮 不愈合的伤 我欠你的 一次都还清 我不赖账 隔着眼泪看世界 仿佛整个宇宙都在哭 你伤心生病寂寞 害怕会有人替我爱护 从此山水不相逢 你要的我给的却不相同 朝露野火不相融 撼动苍穹不再是你的英雄 若灵魂相结在天地之间 看山盟海誓引一场惊觉 没有你的世界 我无力承受 拔情诀爱的最后为了你我绝不退后 指尖缠绕的温柔要我放弃怎么能够 化作一把锋利剑 刺向了我在你身后无处可躲 诀爱 模糊光阴距离光阴距离 连时间都暂停全都暂停 全都只是因为你为了你 确定从一开始就注定 你是心中唯一 说再见来不及后悔已来不及 开不了口的惋惜为了你 来不及 谈什么风花雪月 只想和你天涯倚剑 结局到曲终人散 黯然销魂风在哽咽 他需要一个期限 逼他再重燃气焰 就当是一场历练 即使你有再多依恋来不及  Sun goes down 有多少失意的人 形单影只徘徊酒巷  Life goes on 他看着熙攘的人群 难免会有点惆怅 像是你熟悉的衣装 陌生的地方 模糊的逆光 如今已变成了砒霜 蔓延到心脏 怎么遗忘诀别的爱 Chain  Auto- Argo Chain SON@ ABUCKS杨哲'
    score = score_lyr(lyric)
    print(score)



# from snownlp import SnowNLP
# from pyspark import SparkConf, SparkContext

# def score_lyr(lyr):
#     depart = lyr.split(" ")
#     score = 0
#     for item in depart:
#         if item == '':
#             continue
#         s = SnowNLP(item)
#         score += s.sentiments
#     score = score / len(depart)
#     return str(score)

# def score_comment(comment):
#     depart_comment = comment.split(" @#$#@ ")
#     if len(depart_comment) == 1:
#         return 0.0
#     s = SnowNLP(depart_comment[3])
#     score = s.sentiments
#     return score

# def score_total(id, lyr):
#     score1 = score_lyr(lyr)
#     score2 = score_comment(id)
#     if score2 == 0.0:
#         score2 = score1
#     return str(score1), str(score2)

# def pys():
#     conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
#     sc = SparkContext(conf=conf)
#     lines = sc.textFile("hdfs://fwt:9000/song_info")
#     lines_total = lines.flatMap(lambda line: [(line.split(" @#$#@ ")[0], line.split(" @#$#@ ")[5])]) \
#                       .flatMap(lambda x: [(x[0], score_comment(x[1]), score_lyr(x[1]))]) \
#                       .collect()
#     return lines_total

# if __name__ == "__main__":
#     print(pys())
