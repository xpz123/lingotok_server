import time
import json
import requests

base_url = 'https://openspeech.bytedance.com/api/v1/vc'
appid = "4822083580"
# access_token = "ICPlIxh2QEPMh1otaFjg0AqemFkuyv3a"
# appid = "5616879215"
# access_token = "T-tbHKLs92NxkOomL3jQlcuRVW48JDvZ"
access_token = "ICPlIxh2QEPMh1otaFjg0AqemFkuyv3a"


# language = 'zh-CN'
# file_url = 'https://vdn.vzuu.com/FHD/4f0cd4d6-9a9a-11ef-bc05-fa03f967de9b-v8_f2_t1_le389l8W.mp4?auth_key=1730985590-0-0-778c2d55c0f733f85e67322e79fc8d08&bu=da4bec50&c=avc.8.0&disable_local_cache=1&expiration=1730985590&f=mp4&pu=1513c7c2&v=ali'


# def log_time(func):
#     def wrapper(*args, **kw):
#         begin_time = time.time()
#         func(*args, **kw)
#         print('total cost time = {time}'.format(time=time.time() - begin_time))
#     return wrapper


def call_huoshan_srt(file_url, language="en-US", words_per_line=55):
    response = requests.post(
                 '{base_url}/submit'.format(base_url=base_url),
                 params=dict(
                     appid=appid,
                     language=language,
                     use_itn='True',
                     use_capitalize='True',
                     max_lines=1,
                     words_per_line=words_per_line,
                 ),
                 json={
                    'url': file_url,
                 },
                 headers={
                    'content-type': 'application/json',
                    'Authorization': 'Bearer; {}'.format(access_token)
                 }
             )
    print('submit response = {}'.format(response.text))
    assert(response.status_code == 200)
    assert(response.json()['message'] == 'Success')

    job_id = response.json()['id']
    response = requests.get(
            '{base_url}/query'.format(base_url=base_url),
            params=dict(
                appid=appid,
                id=job_id,
            ),
            headers={
               'Authorization': 'Bearer; {}'.format(access_token)
            }
    )
    assert(response.status_code == 200)
    res = response.json()
    return res
    # print('query response = {}'.format(response.json()))
    # fw = open("485_srt_test.txt", "w")
    # fw.write(json.dumps(response.json()))
    # fw.close()
    # assert(response.status_code == 200)


def call_huoshan_srt_wav(file_path, language="zh-CN", words_per_line=15):
    with open(file_path, 'rb') as recording_file:
        recording_data = recording_file.read()

    response = requests.post(
                 '{base_url}/submit'.format(base_url=base_url),
                 params=dict(
                     appid=appid,
                     language=language,
                     use_itn='True',
                     use_capitalize='True',
                     max_lines=1,
                     words_per_line=words_per_line,
                 ),
                 headers={
                    'content-type': 'audio/wav',
                    'Authorization': 'Bearer; {}'.format(access_token)
                 },
                 data=recording_data
             )
    print('submit response = {}'.format(response.text))
    assert(response.status_code == 200)
    assert(response.json()['message'] == 'Success')

    job_id = response.json()['id']
    response = requests.get(
            '{base_url}/query'.format(base_url=base_url),
            params=dict(
                appid=appid,
                id=job_id,
            ),
            headers={
               'Authorization': 'Bearer; {}'.format(access_token)
            }
    )
    assert(response.status_code == 200)
    res = response.json()
    return res

if __name__ == '__main__':
    print (call_huoshan_srt_wav("/Users/tal/work/lingtok_server/video_process/test.wav"))
    # pass
    # main()