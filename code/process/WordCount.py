#!/usr/bin/env python3
#coding: UTF-8
from pyspark import SparkConf, SparkContext
from visualize import visualize
import jieba
from reptile import Reptile
from snownlp import SnowNLP
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

SRCPATH = '/home/hadoop/danmu/src/'

# conf = SparkConf().setAppName("ex2").setMaster("spark://master:7077")
conf = SparkConf().setAppName("ex2").setMaster("local")
sc = SparkContext(conf=conf)


def getStopWords(stopWords_filePath):
    stopwords = [line.strip() for line in open(
        stopWords_filePath, 'r', encoding='utf-8').readlines()]
    return stopwords


def jiebaCut(answers_filePath):
    """
    结巴分词
    :param answers_filePath: danmu.txt路径
    :return:
    """
    # 读取danmu.txt
    # answersRdd每一个元素对应danmu.txt每一行
    answersRdd = sc.textFile(answers_filePath)

    # 利用SpardRDD reduce()函数,合并所有弹幕
    str = answersRdd.reduce(lambda x, y: x+y)

    # jieba分词
    # 手动添加一些游戏内常用语
    jieba.add_word("纳西妲")
    jieba.add_word("玩到关服")
    jieba.add_word("大贤者")
    jieba.add_word("出必还愿")
    jieba.add_word("教令院")
    jieba.add_word("小草神")
    jieba.add_word("非洲人")
    jieba.add_word("米哈游")
    jieba.add_word("草神")
    jieba.add_word("刀在手")
    jieba.add_word("跟我走")
    jieba.add_word("天动万象")
    jieba.add_word("大慈树王")
    words_list = jieba.lcut(str)
    return words_list


def wordcount(isvisualize=False):
    """
    对所有弹幕进行
    :param visualize: 是否进行可视化
    :return: 将序排序结果RDD
    """
    # 读取停用词表
    stopwords = getStopWords(SRCPATH + 'stop_words.txt')

    # 调用结巴分词
    words_list = jiebaCut("file://" + SRCPATH + "danmu.txt")

    # 词频统计
    wordsRdd = sc.parallelize(words_list)

    # wordcount：去除停用词等同时对最后结果按词频进行排序

    resRdd = wordsRdd.filter(lambda word: word not in stopwords) \
                     .filter(lambda word: len(word) > 1)\
                     .map(lambda word: (word, 1)) \
                     .reduceByKey(lambda a, b: a+b) \
                     .sortBy(ascending=False, numPartitions=None, keyfunc=lambda x: x[1]) \


    # 可视化展示
    if isvisualize:
        v = visualize()
        # 饼状图及柱状图可视化
        pieDic = v.rdd2dic(resRdd, 10)
        v.drawPie(pieDic)
        v.drawBar(pieDic)
        # 词云可视化
        wwDic = v.rdd2dic(resRdd, 1500)
        v.drawWorcCloud(wwDic)
    return resRdd


def get_sentiments():
    print("弹幕情感分析中...")
    text=open("/home/hadoop/danmu/src/danmu.txt")
    sentences=text.readlines()
    size=len(sentences)
    # 划分数据以平滑化情感变化，降低噪音的影响
    div=200
    idx = np.array([i for i in range(int(size/div))])
    scores=np.array([])
    score=0
    cnt=div
    for i in sentences:
        s = SnowNLP(i)
        sentiment=s.sentiments
        # 情感分级处理
        if sentiment <0.3:      #strongly negative
            score+=1
        elif sentiment <0.5:    #weakly negative
            score+=2
        elif sentiment <0.7:    #weakly positive
            score+=3
        else:                   #strongly positive
            score+=4
        cnt-=1
        if cnt==0:  # 当前分组结束时提交得分并重置
            scores=np.append(scores,score)
            score=0
            cnt=div
    
    # 曲线平滑化（插值法）
    idx_new = np.linspace(min(idx),max(idx),10000)
    scores_smooth = make_interp_spline(idx,scores)(idx_new)
    plt.plot(idx_new,scores_smooth)
    
    # plt.plot(idx,scores)
    plt.xlabel("process of danmu")
    plt.xticks([])
    plt.ylabel("score of sentiments")
    plt.yticks([])
    plt.title("The Change Of Sentiments")
    plt.savefig("/home/hadoop/danmu/results/情感分析.png")
    print("弹幕情感分析结束...")
    



if __name__ == '__main__':

    # 爬取B站弹幕
    print("正在爬取B站弹幕...")
    reptile=Reptile()
    # 进行词频统计并可视化
    print("正在进行词频统计与可视化处理...")
    resRdd = wordcount(isvisualize=True)
    # 弹幕情感分析
    get_sentiments()
    # print(resRdd.take(10))  # 查看前10个
