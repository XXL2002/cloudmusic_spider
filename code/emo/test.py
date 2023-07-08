from snownlp import *
from pyspark import SparkConf, SparkContext
import os
os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/envs/pyspark_3.6/bin/python"
# import sys 
# sys.path.append('C:\\Users\\username\\anaconda3\\envs\\automodel_env\\Lib\\site-packages')
# sys.path = list(set(sys.path))
# sys.path


def score_lyr(lyr):
    depart=lyr.split(" ")
    score=0
    for item in lyr:
        if(item == ''):
            lyr.remove('')
            continue
        s=SnowNLP(item)
        score=score+s.sentiments
    score=score/len(lyr)
    return score

# def score_comment(id):
#     song_comment_filename="hdfs://fwt:9000/song_"+id+".txt"
#     score2=0
#     count=0
#     commentlist = []
#     try:
#         read_comments=open(song_comment_filename, 'r', encoding="UTF-8")
#         for line1 in read_comments:
#             depart_comment=line1.split(" @#$#@ ")
#             commentlist.append(depart_comment)
#             if(len(depart_comment)==1):
#                 continue
#             # s=SnowNLP(depart_comment[3])
#             # score2=score2+s.sentiments
#             # count=count+1
#             # print(score2)
#         # score2=score2/count
#     except:
#         # if score2>0.0:
#         #     score2=score2/count
#         # else:
#         #     score2 = 0.0
#         return  commentlist
#     return commentlist

def score_comments(id):
    song_comment_filename="hdfs://fwt:9000/song_"+id+".txt"
    score2=0
    score = []
    try:
        conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
        sc = SparkContext(conf=conf)
        lines=sc.textFile("hdfs://fwt:9000/song_2059775269.txt")
        score = lines.filter(lambda line: len(line.split(" @#$#@ "))>1)\
        .map(lambda line: line.split(" @#$#@ ")[3])\
        .map(lambda x: SnowNLP(x).sentiments)\
        .collect()
    except:
        if len(score)>0:
            score2 = sum(score)/len(score)
        else:
            score2 = 0.0
        return  score2
    score2 = sum(score)/len(score)
    return score2

def score_total(id,lyr):
    score1 = score_lyr(lyr)
    score2 = score_comments(id)
    print(score2)
    if score2 == 0.0:
        score2=score1
    return score1,score2


def pys():
    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile("hdfs://fwt:9000/song_info.txt")
    # lines.distinct()\
    lines_total = lines.map(lambda line: [line.split(" @#$#@ ")[0], line.split(" @#$#@ ")[5]]) \
        .map(lambda x: score_total(str(x[0]), str(x[1])))\
        .collect()
    
    return lines_total


if __name__=="__main__":

    print(pys())


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
