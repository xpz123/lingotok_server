import time
import requests
import os
from datetime import timedelta

base_url = 'https://openspeech.bytedance.com/api/v1/vc/ata'
appid = "4822083580"
access_token = "ICPlIxh2QEPMh1otaFjg0AqemFkuyv3a"

def milliseconds_to_time_string(ms):
    delta = timedelta(milliseconds=ms)

    total_seconds = int(delta.total_seconds())
    
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = delta.microseconds // 1000  # 转换微秒为毫秒
    
    time_string = f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"
    return time_string

def log_time(func):
    def wrapper(*args, **kw):
        begin_time = time.time()
        func(*args, **kw)
        print('total cost time = {time}'.format(time=time.time() - begin_time))
    return wrapper


def huoshan_srt_with_text(audio_text, audio_file):
    response = requests.post(
        '{base_url}/submit'.format(base_url=base_url),
        params=dict(
            appid=appid,
            caption_type='speech',
        ),
        files={
            'audio-text': audio_text,
            'data': (os.path.basename(audio_file), open(audio_file, 'rb'), 'audio/wav'),
        },
        headers={
            'Authorization': 'Bearer; {}'.format(access_token)
        }
    )
    print('submit response = {}'.format(response.text))
    assert (response.status_code == 200)
    assert (response.json()['message'] == 'Success')

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
    # print('query response = {}'.format(response.json()))
    assert (response.status_code == 200)
    return response.json()


if __name__ == '__main__':
    file = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/yunyi.wav"
    audio_text = "中国古代文学发端: 中国是世界文明古国，也是人类的发源地之一。中国到目前为止是世界上发现旧石器时代的人类化石和文化遗址最多的国家，其中重要的有元谋人、蓝田人、北京人、山顶洞人等。中国原始社会从公元前170万年到公元前21世纪。在中国古籍中，有不少关于艺术起源或原始艺术的记述。中国古籍一致认为文学艺术的起源很早。这些记述，揭示了诗歌乐舞与祭祀巫术的密切联系。"
    srt_file = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1.srt"

    ori_resp = huoshan_srt_with_text(audio_text, file)
    # start_time_list = list()
    # end_time_list = list()
    # text_list = list()
    # fw = open(srt_file, "w")
    # for i, utterance in enumerate(ori_resp["utterances"]):
    #     start_time = milliseconds_to_time_string(utterance["start_time"])
    #     start_time_list.append(start_time)
    #     end_time = milliseconds_to_time_string(utterance["end_time"])
    #     end_time_list.append(end_time)
    #     text = utterance["text"]
    #     text_list.append(text)
    
    # for i in range(len(text_list)):
    #     text = text_list[i]
    #     start_time = start_time_list[i]
    #     end_time = end_time_list[i]
    #     en_srt_content = f"{i}\n{start_time} --> {end_time}\n{text}\n\n"
    #     zh_srt_fw.write(en_srt_content)