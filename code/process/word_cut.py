import jieba
import re

# 读取停用词列表
def stopwordslist():
    stopwords = [line.strip() for line in open('/home/cloudmusic_spider/code/tools/stop_words.txt', 'r', encoding='utf-8').readlines()]
    return set(stopwords)   # 转集合，加快速度


# 增加分词
def jieba_addwords():
    jieba.add_word('')


# 对句子进行中文分词
def comment_depart(sentence):

    # 增加指定分词
    jieba_addwords()

    # 进行中文分词
    word_list = jieba.lcut(sentence.strip())
    
    # 读取停用词列表
    stopwords = stopwordslist()

    # 筛选
    result = [word for word in word_list if word not in stopwords and len(word) > 1]

    # 返回最终结果，空格分割的字符串
    return ' '.join(result)



# 对歌词进行处理后分词
def lyric_depart(lyric):

    lyric = re.sub('\[[^\]]*\]', '', lyric).lstrip()    # 去除时间标签

    lyric = re.sub('作词\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('作曲\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('制作人\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('编曲\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('演唱\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('混音/母带\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('混音母带\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('混音&母带工程\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('封面设计\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('原词\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('原曲\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('原唱\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('OP\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('SP\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('音乐总监\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('制作总监\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('乐队总监\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('混音\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('钢琴\s*:\s*\w+\s*', '', lyric)

    lyric = re.sub('一提琴\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('二提琴\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('中提琴\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('大钢琴\s*:\s*\w+\s*', '', lyric)

    lyric = re.sub('和声\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('弦乐录制\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('改编歌曲\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('吉他\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('录音师\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('录音室\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('企划统筹\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('文案\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('经纪公司\s*:\s*\w+\s*', '', lyric)
    lyric = re.sub('合作\s*:\s*\w+\s*', '', lyric)

    # 增加指定分词
    jieba_addwords()

    # 进行中文分词
    word_list = jieba.lcut(sentence.strip())
    
    # 读取停用词列表
    stopwords = stopwordslist()

    # 筛选
    result = [word for word in word_list if word not in stopwords and len(word) > 1]

    # 返回最终结果，空格分割的字符串
    return ' '.join(result)



if __name__=="__main__":

    sentence = '每次考试答辩就会想起这首歌[流泪]：你比我清楚还要我说明白（╯‵□′）╯︵┴─┴'
    str = comment_depart(sentence)
    print(str)