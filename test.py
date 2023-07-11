from collections import Counter


def count_style(str_id, song_style_dict):       # 统计歌单不同风格以及出现的次数

    songid_list = str_id.split(' ')    # 歌单中歌曲id列表
    style_dict = Counter(style for songid in songid_list for style in song_style_dict.get(songid, []))
    result1 = ' '.join([str(key) for key in style_dict.keys()])
    result2 = ' '.join([str(value) for value in style_dict.values()])
    
    return (result1, result2)


str_id = '1 2 3 4 5 6 7 8'
song_style_dict = {'1': ['a', 'b', 'c'], '2': ['g', 'c', 'a'], '3': ['a', 'b']}
result = count_style(str_id, song_style_dict)
print(result)
