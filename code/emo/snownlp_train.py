import snownlp as sn

# sn.sentiment.train(r"E:\neg.txt",r"E:\pos.txt")
# sn.sentiment.save(r'E:\sentiment.marshal')

print(sn.SnowNLP("勉强就没意思了 从你对我不如从前那一刻我们就注定走不到最后.").sentiments)
print(sn.SnowNLP("我在自驾的路上听着这首歌，爽爆！").sentiments)