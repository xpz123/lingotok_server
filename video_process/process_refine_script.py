import pandas as pd
import os
import csv
from translator import translate_text2ar, Translator
from pypinyin import pinyin
from pymongo import MongoClient
from vod_hw_util import show_playinfo_by_assetids, upload_media
import requests
from urllib.parse import urlparse
import time
from tqdm import tqdm
from create_video import update_video_info_by_video_id

mongo_client = MongoClient("mongodb://ruser:Lingotok123!@101.46.54.186:8635,101.46.58.227:8635/test?authSource=admin")
translator = Translator()

def download_file(url, output_path):
    try:
        print(f"开始下载: {url}")
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            
            # 获取文件大小
            total_size = int(response.headers.get('content-length', 0))
            bytes_downloaded = 0
            
            # 下载文件并显示进度
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        file.write(chunk)
                        bytes_downloaded += len(chunk)
                        if total_size > 0:
                            progress = (bytes_downloaded / total_size) * 100
                            print(f"\r下载进度: {progress:.2f}%", end='', flush=True)
            
            print(f"\n下载完成! 文件保存到: {output_path}")
            return True
            
    except Exception as e:
        print(f"下载失败: {e}")
        return False

def download_video(asset_id, video_dir):
    asset_info = show_playinfo_by_assetids([asset_id])
    if len(asset_info) == 0:
        print("asset_info is empty")
        return False
    asset_info = asset_info[0]
    url = asset_info["play_url"]
    
    # 创建视频目录
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    
    # 生成文件名
    filename = f"{asset_id}.mp4"
    output_path = os.path.join(video_dir, filename)

    if os.path.exists(output_path):
        return output_path

    if not download_file(url, output_path):
        return None
    return output_path
    

def process_srt(zh_srt_url, video_id, srt_dir):
    
    srt_file_path = os.path.join(srt_dir, f"{video_id}_Chinese.srt")
    en_srt_file_path = os.path.join(srt_dir, f"{video_id}_English.srt")
    ar_srt_file_path = os.path.join(srt_dir, f"{video_id}_Arabic.srt")
    pinyin_srt_file_path = os.path.join(srt_dir, f"{video_id}_Pinyin.srt")
    
    res = {"zh_srt": srt_file_path, "en_srt": en_srt_file_path, "ar_srt": ar_srt_file_path, "pinyin_srt": pinyin_srt_file_path}
    if not os.path.exists(srt_file_path):
        if not download_file(zh_srt_url, srt_file_path):
            return None
    if not os.path.exists(en_srt_file_path):
        translator.translate_zhsrt2ensrt_with_context(srt_file_path, en_srt_file_path)
    if not os.path.exists(ar_srt_file_path):
        translator.translate_zhsrt2arsrt_huoshan(srt_file_path, ar_srt_file_path)
    if not os.path.exists(pinyin_srt_file_path):
        translator.convert_zhsrt_to_pinyinsrt(srt_file_path, pinyin_srt_file_path)
    return res
    

if __name__ == "__main__":
    db = mongo_client["lingotok"]
    series_collection = db["series"]
    video_collection = db["video"]

    # series_name = "《中国旅游》妞妞"
    # series_name = "《恋与深空》"
    # series_name = "《航拍中国》"
    # series_name = "《舌尖上的中国》"
    # series_name = "《黑猫少女Bella》黑猫少女Bella"
    # series_name = "《妞妞 Lalla香香》妞妞 Lalla香香"
    # series_name = "《不刷题的吴姥姥》不刷题的吴姥姥"
    # series_name = "《房琪kiki》房琪kiki"
    series_name = "《小Lin说》小Lin说"
    # series_name = "《毕的二阶导视频合集》毕的二阶导"
    # series_name = "《明星整容整了哪里》"
    # series_name = "《开饭了大熊猫》开饭了大熊猫"


    print (series_name)
    root_dir = os.path.join("/Users/tal/work/lingtok_server/video_process/refine_videos", series_name)
    video_dir = os.path.join(root_dir, "videos")
    srt_dir = os.path.join(root_dir, "srt")
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    if not os.path.exists(srt_dir):
        os.makedirs(srt_dir)

    cursor = series_collection.find({"name": series_name})
    series_id = None
    for series_info in cursor:
        series_id = str(series_info["_id"])
        print (series_id)
        break
    if series_id is None:
        print ("series_id is None")
        exit()
    
    cursor = video_collection.find({"series_id": series_id, "status": 1})
    video_info_lists = []
    for video_info in cursor:
        video_info_lists.append(video_info)

    for video_info in tqdm(video_info_lists):
        import pdb; pdb.set_trace()
        try:
            video_id = str(video_info["_id"])
            asset_id = str(video_info["asset_id"])
            zh_srt_url = video_info["draft_subtitle_url"]
            if asset_id is None:
                print ("asset_id is None")
                continue
            video_path = download_video(asset_id, video_dir)
            srt_res = process_srt(zh_srt_url, asset_id, srt_dir)
            new_asset_id = upload_media(video_path, srt_res["zh_srt"], srt_res["en_srt"], srt_res["ar_srt"], srt_res["pinyin_srt"], title=video_id)
            update_video_info_by_video_id(video_id, asset_id=new_asset_id, status="online")
        except Exception as e:
            print (e)
            continue

