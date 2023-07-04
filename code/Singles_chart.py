import sys
sys.path.append("code")

from tools.struct import Music_charts
from playlist.get_playlist_info import get_play_list
from playlist.get_playlist_comments import get_playlist_comments
from song.get_song_info import get_song_info
from song.get_song_comments import get_song_comments

if __name__=="__main__":
    
    for chart_id in Music_charts.values():   # 遍历每个排行榜

        playlist_info_path = r'data\playlist_info.txt'
        with open(playlist_info_path, 'a', encoding='utf-8') as file:
            file.write("{},{},{},{},{},{},{}\n".format("user_id", "user_name", "comment_id", "comment", "time", "likecount", "location"))

        playlist = get_play_list(chart_id)     # 爬取排行榜的基本信息

        get_playlist_comments(chart_id)     # 爬取排行榜的评论

        for song_id in playlist['trackIds']:  # 遍历该排行榜中的所有歌曲
            
            song_info_path = r'\data\song_info.txt'
            with open(song_info_path, 'a', encoding='utf-8') as file:
                file.write("{},{},{},{},{},{},{}\n".format("user_id", "user_name", "comment_id", "comment", "time", "likecount", "location"))
            
            get_song_info(song_id)   # 爬取歌曲基本信息
            
            get_song_comments(song_id)      # 爬取歌曲评论
    

        







