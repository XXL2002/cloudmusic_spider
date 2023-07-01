from utils import get
import re

def get_song_lyric(songid):

    url = f'https://music.163.com/api/song/lyric?id={songid}&lv=1&kv=1&tv=-1'
    content_json = get(url)

    lyric = content_json['lrc']['lyric'].replace("\n", "")
    pattern = r"\[[^\]]*\]"

    return re.sub(pattern, " ", lyric)


if __name__ == "__main__":

    lyric = get_song_lyric(1959190717)
    print(lyric)