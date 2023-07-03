import sys
sys.path.append("code")

from tools.request import get, post

def get_user_listen_rank(user_id):
    '''
        获取用户个人听歌排行
    '''

    url = 'https://music.163.com/weapi/v1/play/record?csrf_token=9c8083b02a7f33ddc27dcf3f5349d817'
    data = {'params': 'IE1qBkZ2Ex7zVBC6UqCEm+wre8DiVj2j1giqiQADg8oGWinAt3iScsdfPVT4JylFoT8xa6b3oNFABHwsLX/PqVvCvyl9Qh+LT2f+UHBGvHrZhg/pD61VV/Lj4pAnwE4qJTlW1aHH98MKCfcGDSncvRSw3iFCgh3fCiYAEp8njbMos7dH+xB1646Ff8ggQ8q/ENMhzCkNlVZPiCYDgjI1qKt8DMPFkRm0UKsV3UrkdZ8=',
            'encSecKey':'6e5b9c9efa50307c04348da0425f86ef1464586ffdbaf4eb22b2cc8dc50477dd77547edc4fad547c62783dd13d5aebd36e69eb2c195b311ff483a8f76c460279f3ef2747233f99da8605dcf17a3c82131eb539ffbb8874594ec2bc42ff125463bebca16edfbb956f664e9aa63017baef67cf9780fe9885959dce6208c5bac05f'}
    
    content_json = post(url, data)

    return content_json

if __name__=="__main__":

    content_json = get_user_listen_rank(31475897)
    print(content_json)

