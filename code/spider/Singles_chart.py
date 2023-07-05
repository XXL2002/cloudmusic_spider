import sys
sys.path.append("code")

from tools.struct import Music_charts, file_headers, file_info_paths
from tools.file import add_header
from playlist.get_playlist_info import get_playlist_info
from playlist.get_playlist_comments import get_playlist_comments
from song.get_song_info import get_song_info
from song.get_song_comments import get_song_comments
from singer.get_singer_info import get_singer_info
from user.get_user_info import get_user_info
from tools.progressBar import progress_bar
from tools.sleep import sleep
from tools.file import cleardir
from multiprocessing import Process
from threading import Thread
import multiprocessing as mp

def init():
    cleardir(r'data/info')
    cleardir(r'data/song_comments')
    cleardir(r'data/playlist_comments')
    
    add_header(file_info_paths['playlist'], file_headers['playlist'])
    add_header(file_info_paths['song'], file_headers['song'])
    add_header(file_info_paths['singer'], file_headers['singer'])
    add_header(file_info_paths['user'], file_headers['user'])
    
def anauser(user_id,i,size):
    get_user_info(user_id)
    if ((i+1) % 10 == 0 or i ==size-1):
        progress_bar(i+1,size)
        sleep()

def anasong(track_id,i,size,users):
    print(f"\t\t单曲idx:{i+1}/{size}")
        
    singer_id = get_song_info(track_id)   # 爬取歌曲基本信息

    get_singer_info(singer_id)           # 爬取歌手基本信息

    tmp_users = get_song_comments(track_id)           # 爬取歌曲评论
    # 取每首歌的前10个用户
    tmp_users = tmp_users[0:10] if len(tmp_users)>=10 else tmp_users
    users += tmp_users
    
def analist(chart_id):
    trackIds = get_playlist_info(chart_id)     # 爬取排行榜的基本信息

    users = get_playlist_comments(chart_id)     # 爬取排行榜的评论
    # 取每个歌单的前20个用户
    users = users[0:20] if len(users)>=20 else users

    tracks=[]
    for i in range(0,len(trackIds)):  # 遍历该排行榜中的所有歌曲
        
        track = Process(target=anasong,args=(trackIds[i],i,len(trackIds),users))
        track.start()
        tracks.append(track)

    for p in tracks:
        p.join()
    
    
    # Light对于每个歌单只取20*1+10*10个用户
    users = list(set(users))
    print(users)
    print("\t正在爬取与本排行榜相关的用户信息...")
    for i in range(0,len(users)):
        sleep()
        uid = users[i]
        print(uid)
        us = Process(target=anauser,args=(uid,i,len(users)))
        us.start()
        us.join()
    return

if __name__=="__main__":
    
    init()
    # pool = mp.Pool()
    # pool.map(analist,Music_charts.values())
    
    for chart_id in Music_charts.values():
        
        pl = Process(target=analist,args={chart_id})
        pl.start()
    
    # cleardir(r'data/info')
    # cleardir(r'data/song_comments')
    # cleardir(r'data/playlist_comments')
    
    # add_header(file_info_paths['playlist'], file_headers['playlist'])
    # add_header(file_info_paths['song'], file_headers['song'])
    # add_header(file_info_paths['singer'], file_headers['singer'])
    # add_header(file_info_paths['user'], file_headers['user'])
    
    # for chart_id in Music_charts.values():   # 遍历每个排行榜

    #     trackIds = get_playlist_info(chart_id)     # 爬取排行榜的基本信息

    #     users = get_playlist_comments(chart_id)     # 爬取排行榜的评论

    #     for i in range(0,len(trackIds)):  # 遍历该排行榜中的所有歌曲
            
    #         print(f"\t\t单曲idx:{i+1}/{len(trackIds)}")
            
    #         singer_id = get_song_info(trackIds[i])   # 爬取歌曲基本信息

    #         get_singer_info(singer_id)           # 爬取歌手基本信息

    #         users += get_song_comments(trackIds[i])           # 爬取歌曲评论
        
    #     # Light只取20个用户
    #     users = list(set(users[0:20]))
    #     print("\t正在爬取与本排行榜相关的用户信息...")
    #     for i in range(0,len(users)):
            
    #         get_user_info(users[i])
    #         if ((i+1) % 10 == 0 or i ==len(users)-1):
    #             progress_bar(i+1,len(users))
    #             sleep()