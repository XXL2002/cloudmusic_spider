
def get_song(song_id):  #song_id: str
    # 返回值为包括[歌曲id,歌曲名称,emo指数,歌曲标签集合]的嵌套列表
    # TODO
    return ["111", "t", 0.9, {"华语", "空灵", "平静", "伤感"} ]


def get_songs_by_emo(target_emo):   #target_emo: float
    # 从歌曲库中查找所有emo指数与target_emo相近的歌曲[前100首？]
    # TODO
    
    # 返回值为包括[歌曲id,歌曲名称,emo指数,歌曲标签集合]的嵌套列表
    # TODO
    return [ ["123", "a", 0.7, {"华语", "伤感"} ], ["321", "b", 0.8, {"纯音乐", "空灵"} ], ["456", "c", 0.9, {"电音", "原声大碟"} ] ]

def filter_with_tag(target_song):   #target_song样例: ["111", "t", 0.9, {"华语", "空灵", "平静", "伤感"} ]
    # 根据自身的标签对1中的结果进行二次筛选，保留共同标签较多的歌曲
    
    # 获取一次筛选的歌曲列表
    song_list = get_songs_by_emo(target_song[2])
    
    # 目标歌曲的tag集合
    target_set = target_song[3]
    
    # 歌曲得分字典
    scores = {}
    
    # 打分
    for song in song_list:
        tag_set = song[3]
        # 根据交集打分
        score = len(target_set & tag_set)
        scores[song[0]] = score
    
    # 根据得分排序
    song_list.sort(key=lambda song : scores[song[0]],reverse = True)   
         
    # print(song_list)
    # print(scores)
    
    return song_list




def get_user_rank(user_id):
    # 获取用户最近听歌排行列表[ 周榜5首 + 总榜5首 ]共10首
    # TODO
    return [ ["123", "a", 0.7, {"华语", "悲伤", "空灵"} ], ["123", "a", 0.7, {"华语", "伤感"} ], ["321", "b", 0.8, {"纯音乐", "空灵"} ]]
  
def get_user_hobby(user_id):
    # 用户的喜好来自于他听歌排行的普适标签
    
    # 获取用户听歌排排行列表
    user_rank = get_user_rank(user_id)
    
    # 用户听歌喜好字典
    hobby_dic = {}
    for song in user_rank:
        tag_set = song[3]
        for tag in tag_set:
            if tag in hobby_dic:
                hobby_dic[tag] += 1
            else:
                hobby_dic[tag] = 1
    
    # 转为列表进行排序
    hobby_list = list(hobby_dic.items())
    hobby_list.sort(key = lambda tag: tag[1], reverse=True)
    
    # 生成用户喜好列表
    res_hobby = [hobby_list[i][0] for i in range(min(5,len(hobby_list)))]
    
    # print(res_hobby)
    # print(hobby_list)
    # print(hobby_dic)
    
    return res_hobby
    
def get_user_detail(user_id):
    # 获取用户基本信息[性别,年龄(int),地区,emo指数(float)]
    # TODO
    return ["男", 18, "重庆市", 0.8]

def get_user_profile(user_id):
    # 分析单个用户的用户画像
    hobby = get_user_hobby(user_id)
    detail = get_user_detail(user_id)
    
    # 泛化用户基本信息  [年龄]
    if detail[1] < 18:
        detail[1] = "未成年"
    elif detail[1] < 30:
        detail[1] = "青年"
    elif detail[1] < 50:
        detail[1] = "中年"
    else:
        detail[1] = "老年"
    # 泛化用户基本信息  [emo指数]
    if detail[3] >= 0.5:
        detail[3] = "重度emo"
    else :
        detail[3] = "轻度emo"
    
    # 根据喜好和泛化基本信息生成用户画像
    user_profile = hobby + detail
    
    # print(user_profile)
    
    return user_profile 

def get_related_users(song_id):
    # 在该歌曲下评论的用户群体
    # TODO
    return ["111", "123", "321"]

def get_song_profile(song_id, isDic = True):
    # 对各听众用户画像进行汇总[字典统计]，找出较为普适的用户画像作为本歌的用户画像
    
    # 获取相关用户群体
    users = get_related_users(song_id)
    # 用户群体画像字典
    users_profile_dic = {}
    for user_id in users:
        profile = get_user_profile(user_id)
        for tag in profile:
            if tag in users_profile_dic:
                users_profile_dic[tag] += 1
            else:
                users_profile_dic[tag] = 1
    
    # 转为列表进行排序
    users_profile = list(users_profile_dic.items())
    users_profile.sort(key = lambda tag: tag[1], reverse=True)
    # print(users_profile)
    
    if (isDic):
        # 生成单曲用户群体画像[字典形式,便于为标签赋权]
        res_profile = {tag[0]:tag[1] for tag in users_profile[:10]}
        
        # print(res_profile)
        
        return res_profile  # dic
    
    else:
        # 生成单曲用户群体画像[列表形式]
        res_profile = [users_profile[i][0] for i in range(min(10,len(users_profile)))]
        
        # print(res_profile)
        
        return res_profile  # list

def refilter_with_profile(song_id):
    # 根据歌曲听众群体画像对初筛结果做最后一次筛选，并推荐得分较高的Top n首歌
    
    # ========初筛========
    # 获取原歌曲
    target_song = get_song(song_id)
    # 基于歌曲本身进行分析,生成候选歌单
    candidate_list = filter_with_tag(target_song)
    
    # ========再筛========
    # 获取本歌听众的群体用户画像
    profile_dic = get_song_profile(song_id)
    
    # 对初筛结果打分
    # 歌曲得分字典
    scores = {}
    for song in candidate_list:     # song format: ["123", "a", 0.7, {"华语", "悲伤", "空灵"} ]
        candidate_profile = get_song_profile(song[0], isDic=False)
        
        score = 0
        for tag in candidate_profile:
            # 标签符合用户画像则带权重加分
            if tag in profile_dic:
                score += profile_dic[tag]
        scores[song[0]] = score
    
    # 根据得分为候选歌曲进行排序
    candidate_list.sort(key = lambda song: scores[song[0]], reverse=True)
    
    # 生成最终的推荐歌曲
    final_list = candidate_list[:10]
    
    print(final_list)
    
    return final_list       
    
def entry(song_id):
    # 入口函数,便于调用
    return refilter_with_profile(song_id)

if __name__ == "__main__":
    # # 获取原歌曲
    # target_song = get_song("123")
    
    # # 基于歌曲本身进行分析
    # filter_with_tag(target_song)
    
    entry("111")