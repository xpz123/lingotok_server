import uuid
from typing import Optional, Union, List

import uvicorn
from fastapi import FastAPI, Body
from pydantic import BaseModel, HttpUrl
import asyncio
from video_processor import VideoProcessor, merge_audios, milliseconds_to_time_string
import os
# os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from tenacity import retry, stop_after_attempt, wait_fixed
import time
import requests
from create_video import sha256_encrypt
import random as rd
from translator import Translator
from huoshan_tts_util import generate_wav
from moviepy.editor import VideoFileClip
from vod_hw_util import upload_media
import sys
from concurrent.futures import ThreadPoolExecutor
import functools
import json

video_processor = VideoProcessor()
translator = Translator()

app = FastAPI()

# 创建线程池执行器
thread_pool = ThreadPoolExecutor(max_workers=4)  # 可以根据服务器性能调整worker数量
task_status = {}

async def run_in_threadpool(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(thread_pool, functools.partial(func, *args, **kwargs))

def download_video(url, output_path=None, chunk_size=1024*1024):
    """
    从指定URL下载视频文件

    参数:
        url (str): 视频的下载URL
        output_path (str, optional): 保存的文件路径。如果为None，则从URL中推导文件名
        chunk_size (int, optional): 每次下载的数据块大小，默认为1MB

    返回:
        bool: 下载成功返回True，失败返回False
    """
    try:
        # 解析URL获取文件名（如果未指定输出路径）
        if output_path is None:
            # 从URL中提取文件名
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                filename = "video_" + str(int(time.time())) + ".mp4"
            output_path = filename

        # 创建保存目录（如果不存在）
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 发送HTTP请求并获取响应
        print(f"开始下载: {url}")
        with requests.get(url, stream=True) as response:
            # 检查请求是否成功
            response.raise_for_status()

            # 获取文件大小（如果服务器提供）
            total_size = int(response.headers.get('content-length', 0))
            bytes_downloaded = 0

            # 下载文件并显示进度
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)
                        bytes_downloaded += len(chunk)
                        # 计算下载进度
                        if total_size > 0:
                            progress = (bytes_downloaded / total_size) * 100
                            sys.stdout.write(f"\r下载进度: {progress:.2f}% ({bytes_downloaded/1024/1024:.2f}MB/{total_size/1024/1024:.2f}MB)")
                            sys.stdout.flush()
                        else:
                            sys.stdout.write(f"\r下载中: {bytes_downloaded/1024/1024:.2f}MB")
                            sys.stdout.flush()

            print("\n下载完成!")
            print(f"文件保存位置: {os.path.abspath(output_path)}")
            return True

    except requests.exceptions.MissingSchema:
        print(f"错误: 无效的URL - {url}")
    except requests.exceptions.ConnectionError:
        print(f"错误: 无法连接到服务器 - {url}")
    except requests.exceptions.Timeout:
        print(f"错误: 请求超时 - {url}")
    except requests.exceptions.HTTPError as e:
        print(f"错误: HTTP请求失败 - {e}")
    except Exception as e:
        print(f"错误: 下载过程中发生异常 - {e}")
    return False

def extract_word_from_llm_res(res):
    word_candidates = []
    char_candidates = []
    for item in res:
        if item["timestamp"] < 0.2:
            continue
        if len(item["word"]) > 1:
            word_candidates.append((item["timestamp"], item["word"]))
        else:
            char_candidates.append((item["timestamp"], item["word"]))
    
    rd.shuffle(word_candidates)
    rd.shuffle(char_candidates)
    if len(word_candidates) > 0:
        start_time = word_candidates[0][0]
        word = word_candidates[0][1]
    else:
        start_time = char_candidates[0][0]
        word = char_candidates[0][1]
    return start_time, word


def generate_subtitle(gen_word_result, srt_prefix, srt_dir, start_time, end_time):
    word = gen_word_result["zh"]
    res = dict()
    zh_srt = os.path.join(srt_dir, "{}_Chinese.srt".format(srt_prefix))
    ar_srt = os.path.join(srt_dir, "{}_Arabic.srt".format(srt_prefix))
    en_srt = os.path.join(srt_dir, "{}_English.srt".format(srt_prefix))
    pinyin_srt = os.path.join(srt_dir, "{}_Pinyin.srt".format(srt_prefix))

    res["zh_srt"] = zh_srt
    res["ar_srt"] = ar_srt
    res["en_srt"] = en_srt
    res["pinyin_srt"] = pinyin_srt
    
    ar_word = gen_word_result["ar"]
    en_word = gen_word_result["en"]

    fw_zh = open(zh_srt, "w")
    fw_ar = open(ar_srt, "w")
    fw_en = open(en_srt, "w")
    
    start_time_str = milliseconds_to_time_string(start_time)
    end_time_str = milliseconds_to_time_string(end_time)
    zh_srt_content = f"{0}\n{start_time_str} --> {end_time_str}\n{word}\n\n"
    fw_zh.write(zh_srt_content)

    ar_srt_content = f"{0}\n{start_time_str} --> {end_time_str}\n{ar_word}\n\n"
    fw_ar.write(ar_srt_content)

    en_srt_content = f"{0}\n{start_time_str} --> {end_time_str}\n{en_word}\n\n"
    fw_en.write(en_srt_content)
    fw_zh.close()
    fw_ar.close()
    fw_en.close()

    video_processor.convert_zhsrt_to_pinyinsrt(zh_srt, pinyin_srt)
    return res


@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
def callback(self_video_id, gen_word_finished=None, gen_word_result=None, gen_word_position=None,gen_video_finished=None, gen_video_asset_id=None, log_file=None):
    req = { "self_video_id": self_video_id}
    if gen_word_finished is not None:
        req["gen_word_finished"] = gen_word_finished
    if gen_word_result is not None:
        req["gen_word_result"] = gen_word_result
    if gen_word_position is not None:
        req["gen_word_position"] = gen_word_position
    if gen_video_finished is not None:
        req["gen_video_finished"] = gen_video_finished
    if gen_video_asset_id is not None:
        req["gen_video_asset_id"] = gen_video_asset_id
    timestamp = str(int(time.time()))
    if log_file is not None:
        with open(log_file, "a") as f:
            f.write(json.dumps(req))
    ori_str = "{}{}{}".format("self_video_process_callback", timestamp, "lingotok")
    signature_256 = sha256_encrypt(ori_str)
    url = "https://api.lingotok.ai/api/v1/self_video/self_video_process_callback"
    response = requests.post(url, json=req, headers={"Content-Type": "application/json", "Timestamp": timestamp, "Signature": signature_256})
    return response.json()

class VideoAddTextCompleteRequest(BaseModel):
    self_video_id: str
    origin_video: str

class VideoAddTextHalfRequest(BaseModel):
    self_video_id: str
    origin_video: str
    gen_word_result: dict
    gen_word_position: int
    

async def process_video_half_async(self_video_id, origin_video_path, gen_word_result, gen_word_start_time, origin_video_url=None):
    try:
        # 将耗时操作包装在run_in_threadpool中
        def process_video():
            root_dir = "tmp/{}".format(self_video_id)
            log_path = os.path.join(root_dir, "log.txt")
            if origin_video_url is not None:
                origin_video_path = os.path.join(root_dir, "ori_video.mp4")
                if not download_video(origin_video_url, origin_video_path):
                    callback(self_video_id, gen_video_finished=False)
                    return {"msg": "视频下载失败", "code": -1}

            # 生成音频
            ori_audio_path = os.path.join(root_dir, "audio.wav")
            merged_audio_path = os.path.join(root_dir, "merged_audio.wav")
            generate_wav(gen_word_result["zh"], ori_audio_path)
            repeat_num = 5
            audio_list = [ori_audio_path] * repeat_num 
            audio_dur_dict = merge_audios(audio_list, merged_audio_path, sil_dur=500)
            audio_dur = 0
            for key in audio_dur_dict.keys():
                audio_dur += audio_dur_dict[key]
            audio_dur = audio_dur * repeat_num
            with open(log_path, "a") as f:
                f.write("audio generated success\n")



            # 重新压制视频
            out_video_path = os.path.join(root_dir, "output.mp4")
            start_video_path = os.path.join(root_dir, "start_video.mp4")
            end_video_path = os.path.join(root_dir, "end_video.mp4")
            insert_video_path = os.path.join(root_dir, "insert_video.mp4")

            video_clip = VideoFileClip(origin_video_path)
            # video_clip = video_processor.add_audio_to_videoclip(video_clip, merged_audio_path, float(gen_word_start_time) / 1000.0, audio_dur)
            # video_clip = video_processor.add_zhword_to_videoclip(video_clip, gen_word_result["zh"], gen_word_start_time, audio_dur)
            # video_clip = video_processor.add_process_bar_to_videoclip(video_clip, gen_word_start_time, audio_dur)
            insert_clip = video_processor.add_audio_to_videoclip_v1(video_clip, merged_audio_path, float(gen_word_start_time) / 1000.0, audio_dur)
            insert_clip = video_processor.add_zhword_to_videoclip(insert_clip, gen_word_result["zh"], 0, audio_dur)
            insert_clip = video_processor.add_process_bar_to_videoclip(insert_clip, 0, audio_dur)
            insert_clip.fps = video_clip.fps
            insert_clip.write_videofile(insert_video_path, codec="libx264", audio_codec="aac")


            # ffmpeg_path = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
            ffmpeg_path = "ffmpeg"
            end_time_str = milliseconds_to_time_string(gen_word_start_time).replace(",", ".")
            ffmpeg_cmd = "{} -ss 00:00:00 -i \"{}\" -to {} -c:v copy -c:a copy -y \"{}\"".format(ffmpeg_path, origin_video_path, end_time_str, start_video_path)
            os.system(ffmpeg_cmd)

            ffmpeg_cmd = "{} -ss {} -i \"{}\" -c:v copy -c:a copy -y \"{}\"".format(ffmpeg_path, end_time_str, origin_video_path, end_video_path)
            os.system(ffmpeg_cmd)

            ffmpeg_cmd = "{} -i {} -i {} -i {} -y -filter_complex \"[0:v:0][0:a:0][1:v:0][1:a:0][2:v:0][2:a:0]concat=n=3:v=1:a=1[outv][outa]\" -map \"[outv]\" -map \"[outa]\" -c:v libx264 -crf 23 -preset fast -c:a aac -b:a 128k \"{}\"".format(ffmpeg_path, start_video_path, insert_video_path, end_video_path, out_video_path)
            os.system(ffmpeg_cmd)

            with open(log_path, "a") as f:
                f.write("video generated success\n")

            # 生成字幕
            srt_res = generate_subtitle(gen_word_result, self_video_id, root_dir, gen_word_start_time, audio_dur)
            with open(log_path, "a") as f:
                f.write("subtitle generated success\n")    

            # 上传视频
            asset_id =upload_media(out_video_path, zh_srt_path=srt_res["zh_srt"], en_srt_path=srt_res["en_srt"], ar_srt_path=srt_res["ar_srt"], py_srt_path=srt_res["pinyin_srt"], title=self_video_id, description="")
            with open(log_path, "a") as f:
                f.write("upload media success\n asset_id: {}\n".format(asset_id))
            callback(self_video_id, gen_video_finished=True, gen_video_asset_id=asset_id, log_file=log_path)
            return {"msg": "视频处理成功", "code": 200}

        result = await run_in_threadpool(process_video)
        return result
    except Exception as e:
        callback(self_video_id, gen_video_finished=False)
        return {"msg": "视频处理失败", "code": -1}
    

def process_video_half(self_video_id, origin_video_path, gen_word_result, gen_word_start_time, origin_video_url=None):
    try:
        root_dir = "tmp/{}".format(self_video_id)
        log_path = os.path.join(root_dir, "log.txt")
        if origin_video_url is not None:
            origin_video_path = os.path.join(root_dir, "ori_video.mp4")
            if not download_video(origin_video_url, origin_video_path):
                callback(self_video_id, gen_video_finished=False)
                return {"msg": "视频下载失败", "code": -1}

        # 生成音频
        ori_audio_path = os.path.join(root_dir, "audio.wav")
        merged_audio_path = os.path.join(root_dir, "merged_audio.wav")
        generate_wav(gen_word_result["zh"], ori_audio_path)
        repeat_num = 5
        audio_list = [ori_audio_path] * repeat_num 
        audio_dur_dict = merge_audios(audio_list, merged_audio_path, sil_dur=500)
        audio_dur = 0
        for key in audio_dur_dict.keys():
            audio_dur += audio_dur_dict[key]
        audio_dur = audio_dur * repeat_num
        # print ("audio generated success")
        with open(log_path, "a") as f:
            f.write("audio generated success\n")

        # 重新压制视频
        out_video_path = os.path.join(root_dir, "output.mp4")
        start_video_path = os.path.join(root_dir, "start_video.mp4")
        end_video_path = os.path.join(root_dir, "end_video.mp4")
        insert_video_path = os.path.join(root_dir, "insert_video.mp4")

        video_clip = VideoFileClip(origin_video_path)
        # video_clip = video_processor.add_audio_to_videoclip(video_clip, merged_audio_path, float(gen_word_start_time) / 1000.0, audio_dur)
        # video_clip = video_processor.add_zhword_to_videoclip(video_clip, gen_word_result["zh"], gen_word_start_time, audio_dur)
        # video_clip = video_processor.add_process_bar_to_videoclip(video_clip, gen_word_start_time, audio_dur)
        insert_clip = video_processor.add_audio_to_videoclip_v1(video_clip, merged_audio_path, float(gen_word_start_time) / 1000.0, audio_dur)
        insert_clip = video_processor.add_zhword_to_videoclip(insert_clip, gen_word_result["zh"], 0, audio_dur)
        insert_clip = video_processor.add_process_bar_to_videoclip(insert_clip, 0, audio_dur)
        insert_clip.fps = video_clip.fps
        insert_clip.write_videofile(insert_video_path, codec="libx264", audio_codec="aac")


        # ffmpeg_path = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
        ffmpeg_path = "ffmpeg"
        end_time_str = milliseconds_to_time_string(gen_word_start_time).replace(",", ".")
        ffmpeg_cmd = "{} -ss 00:00:00 -i \"{}\" -to {} -c:v copy -c:a copy -y \"{}\"".format(ffmpeg_path, origin_video_path, end_time_str, start_video_path)
        os.system(ffmpeg_cmd)

        ffmpeg_cmd = "{} -ss {} -i \"{}\" -c:v copy -c:a copy -y \"{}\"".format(ffmpeg_path, end_time_str, origin_video_path, end_video_path)
        os.system(ffmpeg_cmd)

        ffmpeg_cmd = "{} -i {} -i {} -i {} -y -filter_complex \"[0:v:0][0:a:0][1:v:0][1:a:0][2:v:0][2:a:0]concat=n=3:v=1:a=1[outv][outa]\" -map \"[outv]\" -map \"[outa]\" -c:v libx264 -crf 23 -preset fast -c:a aac -b:a 128k \"{}\"".format(ffmpeg_path, start_video_path, insert_video_path, end_video_path, out_video_path)
        os.system(ffmpeg_cmd)

        # print ("video generated success")
        with open(log_path, "a") as f:
            f.write("video generated success\n")

        # 生成字幕
        srt_res = generate_subtitle(gen_word_result, self_video_id, root_dir, gen_word_start_time, audio_dur)
        # print ("subtitle generated success")    

        # 上传视频
        asset_id =upload_media(out_video_path, zh_srt_path=srt_res["zh_srt"], en_srt_path=srt_res["en_srt"], ar_srt_path=srt_res["ar_srt"], py_srt_path=srt_res["pinyin_srt"], title=self_video_id, description="")
        # print ("upload media success")
        with open(log_path, "a") as f:
            f.write("upload media success\n asset_id: {}\n".format(asset_id))
        callback(self_video_id, gen_video_finished=True, gen_video_asset_id=asset_id, log_file=log_path)
        return {"msg": "视频处理成功", "code": 200}
    except Exception as e:
        callback(self_video_id, gen_video_finished=False, log_file=log_path)
        return {"msg": "视频处理失败", "code": -1}

async def process_video_complete(self_video_id, origin_video):
    try:
        root_dir = "tmp/{}".format(self_video_id)
        os.makedirs(root_dir, exist_ok=True)
        frame_dir = os.path.join(root_dir, "frames")
        os.makedirs(frame_dir, exist_ok=True)
        ori_video_path = os.path.join(root_dir, "ori_video.mp4")
        log_path = os.path.join(root_dir, "log.txt")
        def process_video():
            if not download_video(origin_video, ori_video_path):
                callback(self_video_id, gen_word_finished=False)
                return {"msg": "视频下载失败", "code": -1}
            try:
                res = video_processor.extract_frames_from_video(ori_video_path, frame_dir, extract_word=True, frame_interval=15, end_time=4.0)
                start_time, word = extract_word_from_llm_res(res)
                start_time = int(start_time * 1000)
                translated_word = translator.translate_zhword(word)
                ar_word = translated_word["ar"]
                en_word = translated_word["en"]
                gen_word_result = {"zh": word, "pinyin": "", "ar": ar_word, "en": en_word}
                callback(self_video_id, gen_word_finished=True, gen_word_result=gen_word_result, gen_word_position=start_time)
                with open(log_path, "a") as f:
                    f.write("gen_word_result: {}\n".format(gen_word_result))
                
            except Exception as e:
                callback(self_video_id, gen_word_finished=False)
                return {"msg": "视频提取文字失败", "code": -1}
            try:
                with open(log_path, "a") as f:
                    f.write("processing video half\n")
                process_video_half(self_video_id, ori_video_path, gen_word_result, start_time)
            except Exception as e:
                callback(self_video_id, gen_video_finished=False)
                return {"msg": "视频处理失败", "code": -1}
            return {"msg": "视频处理成功", "code": 200}
        
        result = await run_in_threadpool(process_video)
        return result
    except Exception as e:
        callback(self_video_id, gen_word_finished=False)
        return {"msg": "视频处理失败", "code": -1}


@app.post('/trigger_self_video_process')
async def trigger_self_video_process(input_data: VideoAddTextCompleteRequest):
    if not os.path.exists("tmp"):
        os.makedirs("tmp")
    
    # 创建异步任务
    task = asyncio.create_task(process_video_complete(input_data.self_video_id, input_data.origin_video))
    
    # 存储任务状态
    task_id = id(task)
    task_status[task_id] = {
        "self_video_id": input_data.self_video_id,
        "status": "processing"
    }
    
    # 添加任务完成回调
    task.add_done_callback(
        lambda t: task_status.update({id(t): {"status": "completed", "result": t.result()}})
    )
    
    return {"task_id": task_id, "self_video_id": input_data.self_video_id}


@app.post("/trigger_self_video_process_half")
async def trigger_self_video_process_half(input_data: VideoAddTextHalfRequest):
    if not os.path.exists("tmp"):
        os.makedirs("tmp")
    
    # 创建异步任务
    task = asyncio.create_task(process_video_half_async(
        input_data.self_video_id,
        "",
        input_data.gen_word_result,
        input_data.gen_word_position,
        input_data.origin_video
    ))
    
    # 存储任务状态
    task_id = id(task)
    task_status[task_id] = {
        "self_video_id": input_data.self_video_id,
        "status": "processing"
    }
    
    # 添加任务完成回调
    task.add_done_callback(
        lambda t: task_status.update({id(t): {"status": "completed", "result": t.result()}})
    )
    
    return {"task_id": task_id, "self_video_id": input_data.self_video_id}


# 添加一个新的状态查询接口
@app.get("/check_task_status/{task_id}")
async def check_task_status(task_id: int):
    if task_id not in task_status:
        return {"error": "任务不存在"}
    return task_status[task_id]


if __name__ == "__main__":
    uvicorn.run(app=app, host='0.0.0.0', port=80)  # http_server = make_server('127.0.0.1', 5000, app)  # http_server.serve_forever()
