from snownlp import SnowNLP
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row


def score_user():
    conf = SparkConf().setMaster("spark://cons:7077").setAppName("job01")
    sc = SparkContext(conf=conf)
    lines=sc.textFile("hdfs://cons:9000/data/info/user_info.txt")

    lines_user_info=lines.distinct()\
                            .map(lambda line: line.split(" @#$#@ "))\
                            .map(lambda list: [list[0],list[1],list[5],list[6],list[7]])\
                            .map(lambda list: [list[i] if i != 2 else score_sig(list[i]) for i in range(len(list))])\
                            .map(lambda list: [list[0],list[1],list[2],list[3].split(" "),list[4].split(" ")])\
                            .map(lambda list: [list[i] if i < 3 else score_song(list[i]) for i in range(len(list))] )\
                            .map(lambda list: [list[i] if list[i] != "0.5" else list[2] for i in range(len(list))])\
                            .collect()
    print(lines_user_info)


def score_sig(sig):
    s=SnowNLP(sig)
    return s.sentiments


def score_lyr(lyr):
    depart=lyr.split(" ")
    score=0
    for item in lyr:
        if(item == ''):
            lyr.remove('')
            continue
        s=SnowNLP(item)
        # s=snownlp.SnowNLP(item)
        score=score+s.sentiments
    score=score/len(lyr)
    return(str(score))


def score_comment(id):
    song_comment_filename="hdfs://fwt:9000/song_"+id
    # song_comment_filename="wyy\emo\data\song_comments\song_2059815659.txt"  
    score2=0
    count=0
    try:
        read_comments=open(song_comment_filename, 'r', encoding="UTF-8")
        for line1 in read_comments:
            depart_comment=line1.split(" @#$#@ ")
            if(len(depart_comment)==1):
                continue
            s=SnowNLP(depart_comment[3])
            score2=score2+s.sentiments
            count=count+1
        score2=score2/count
    except:
        score2=0.0

def score_total(id,lyr):
    score1 = score_lyr(lyr)
    score2 = score_comment(id)
    if score2 == 0.0:
        score2=score1
    return score1,score2

# def score_lyric(lyr):
#     lyr_list=lyr.split(" ")
#     for item in lyr_list:
#         if(item==' '):
#             lyr_list.remove(' ')
#             continue
#         s=SnowNLP(item)
#         score1=score1+s.sentiments
#     score1=score1/len(lyr)
#     return score1

# def score_comment(song_id):
#     conf = SparkConf().setMaster("spark://cons:7077").setAppName("job03")
#     sc = SparkContext(conf=conf)
#     filepath="hdfs://cons:9000/data/song_comments/song_"+ song_id +".txt"
#     lines=sc.textFile(filepath)
#     line_score_com=
#     return 0

def score_song(song_id):
    if(song_id==""):
        return 0.5
    conf = SparkConf().setMaster("spark://cons:7077").setAppName("job02")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    rdd_file = sc.textFile("hdfs://cons:9000/data/info/song_info.txt")
    lines_song_info = rdd_file.distinct()\
                                .map(lambda record: record.split(","))\
                                .map(lambda list: list[list[0],list[5]])\
                                .collect()
    df = lines_song_info.map(lambda line: Row(song_id=line[0], lyr=int(line[1]))).toDF()
    df.createOrReplaceTempView("t_table")
    score_lyr,score_com=score_total(song_id,spark.sql("select lyr from t_table where t_table.songid="+str(song_id)))
    return score_com*0.5+score_lyr*0.5
    

    # lines_song_info=lines.distinct()\
    #                         .map(lambda line: line.split(" @#$#@ "))\
    #                         .map(lambda list: [list[0],list[1],list[5]])\
    #                         .collect()
                            # .map(lambda list: [list[i] if i != 2 else socre_lyr(list[i]) for i in range(len(list))])
    

if __name__=="__main__":
    score_user()