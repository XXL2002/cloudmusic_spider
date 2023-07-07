from cnsenti import Emotion,Sentiment
# import 

emotion = Emotion()
senti = Sentiment()
test_text = '高中时候有一天心情不好，一个女生就给我唱了这首歌。后来顺理成章的在一起。初恋，我还不懂爱情。因为一点小事就要闹分手，删除了所有她的联系方式。她现在已经遇到了能给她想要的人，我却再也没能遇见能打开我心墙的人。其实我现在只想对她说声抱歉，真心的祝福她。是的，我很想她.'
result = emotion.emotion_count(test_text)
print(result)
result = senti.sentiment_count(test_text)
print(result)