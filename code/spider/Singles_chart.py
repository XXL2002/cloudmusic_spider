import sys
sys.path.append("code")

from tools.struct import Music_charts, file_headers, file_info_paths
from tools.file import add_header
from playlist.get_playlist_info import get_playlist_info
from playlist.get_playlist_comments import get_playlist_comments
from song.get_song_info import get_song_info
from song.get_song_comments import get_song_comments
from singer.get_singer_info import get_singer_info


if __name__=="__main__":
    
    add_header(file_info_paths['playlist'], file_headers['playlist'])
    add_header(file_info_paths['song'], file_headers['song'])
    add_header(file_info_paths['singer'], file_headers['singer'])
    add_header(file_info_paths['user'], file_headers['user'])
    
    for chart_id in Music_charts.values():   # 遍历每个排行榜

        trackIds = get_playlist_info(chart_id)     # 爬取排行榜的基本信息

        get_playlist_comments(chart_id)     # 爬取排行榜的评论

        for song_id in trackIds:  # 遍历该排行榜中的所有歌曲
            
            singer_id = get_song_info(song_id)   # 爬取歌曲基本信息

            get_singer_info(singer_id)           # 爬取歌手基本信息

            get_song_comments(song_id)           # 爬取歌曲评论