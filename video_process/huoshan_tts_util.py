#coding=utf-8

'''
requires Python 3.6 or later
pip install requests
'''
import base64
import json
import uuid
import requests

# 填写平台申请的appid, access_token以及cluster
appid = "4822083580"
access_token= "ICPlIxh2QEPMh1otaFjg0AqemFkuyv3a"
cluster = "volcano_tts"

# voice_type = "BV001_streaming"
host = "openspeech.bytedance.com"
api_url = f"https://{host}/api/v1/tts"

header = {"Authorization": f"Bearer;{access_token}"}

def generate_wav(text, audio_file, voice_type="BV001_streaming", speed=0.9):
    request_json = {
        "app": {
            "appid": appid,
            "token": "access_token",
            "cluster": cluster
        },
        "user": {
            "uid": "388808087185088"
        },
        "audio": {
            "voice_type": voice_type,
            "encoding": "wav",
            "speed_ratio": 0.9,
            "volume_ratio": 1.0,
            "pitch_ratio": 1.0,
        },
        "request": {
            "reqid": str(uuid.uuid4()),
            "text": text,
            "text_type": "plain",
            "operation": "query",
            "with_frontend": 1,
            "frontend_type": "unitTson"

        }
    }
    try:
        resp = requests.post(api_url, json.dumps(request_json), headers=header)
        # print(f"resp body: \n{resp.json()}")
        if "data" in resp.json():
            data = resp.json()["data"]
            file_to_save = open(audio_file, "wb")
            file_to_save.write(base64.b64decode(data))
    except Exception as e:
        print (str(e))

if __name__ == '__main__':
    generate_wav("你好", "test.wav", voice_type="BV001_streaming", speed=0.9)
    generate_wav("你好", "test2.wav", voice_type="BV001_streaming", speed=0.8)
    generate_wav("你好", "test3.wav", voice_type="BV002_streaming", speed=0.9)
    generate_wav("你好", "test4.wav", voice_type="BV002_streaming", speed=0.8)