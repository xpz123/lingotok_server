import requests
import json
import pandas as pd
from tqdm import tqdm
import time
import hashlib
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import redis
from tenacity import retry, stop_after_attempt, wait_fixed

mongo_client = MongoClient("mongodb://ruser:Lingotok123!@101.46.54.186:8635,101.46.58.227:8635/test?authSource=admin")

def sha256_encrypt(data):
    # 将数据转换为字节流
    data_bytes = data.encode('utf-8')
    
    # 创建SHA256对象并更新数据
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data_bytes)
    
    # 获取加密后的哈希值
    hashed_data = sha256_hash.hexdigest()
    
    return hashed_data

def update_videoinfo_recommender_withcsv(csv_file):
    df = pd.read_csv(csv_file)
    url = "http://localhost:5000/update_recommender_video_info"
    for i in tqdm(range(df.shape[0])):
        try:
            data = df.iloc[i].to_dict()
            new_data = dict()
            for k in data.keys():
                if str(data[k]) != "nan":
                    new_data[k] = data[k]
                else:
                    new_data[k] = None
            response = requests.post(url, json=new_data)
        except Exception as e:
            print (e)
            print ("error in {}".format(df.iloc[i]["title"]))

def create_video_internal(asset_id, title, duration, quiz_list, sub_list, level, audio_ratio, customize=None):
    # quiz_lang = {"language": "zh", "question": "测试问题?", "option_list": ["选项1", "选项2", "选项3", "选项4"], "answer_list": ["选项3"], "explanation": "解释"}
    # quiz = {"quiz_id": "1", "quiz_type": "single_choice", "quiz_language_list": [quiz_lang]}
    # sub_zh = {"language": "zh", "obs_object": "中文字幕.srt"}
    level = int(str(level).replace("HSK", ""))
    req = {"asset_id": asset_id, "title": title, "duration": duration, "level": level, "audio_radio": audio_ratio, "quiz_list": quiz_list, "subtitle_list": sub_list}
    if customize is not None:
        req["customize"] = customize
    # req_str = json.dumps(req, ensure_ascii=False)
    # print (json.dumps(req, ensure_ascii=False))
    url = "https://api.lingotok.ai/api/v1/video/create_video_info"
    response = requests.post(url, json=req, headers={"Content-Type": "application/json", "authorization": "skip_auth"})
    # print (response.json()["video_id"])
    return response.json()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
def create_video_internal_new(asset_id, title, duration, quiz_list, sub_list, level, audio_ratio, customize=None, series_name=None):
    # quiz_lang = {"language": "zh", "question": "测试问题?", "option_list": ["选项1", "选项2", "选项3", "选项4"], "answer_list": ["选项3"], "explanation": "解释"}
    # quiz = {"quiz_id": "1", "quiz_type": "single_choice", "quiz_language_list": [quiz_lang]}
    # sub_zh = {"language": "zh", "obs_object": "中文字幕.srt"}
    level = int(str(level).replace("HSK", ""))
    audio_ratio = float(audio_ratio)
    req = {"asset_id": asset_id, "title": title, "duration": duration, "level": level, "audio_radio": audio_ratio, "quiz_list": quiz_list, "subtitle_list": sub_list}
    # req_str = json.dumps(req, ensure_ascii=False)
    # print (req_str)
    # req_str = json.dumps(req, ensure_ascii=False)
    # print (json.dumps(req, ensure_ascii=False))
    if customize is not None:
        req["customize"] = customize
    if series_name is not None:
        req["series_name"] = series_name
    timestamp = str(int(time.time()))
    ori_str = "{}{}{}".format("create_video_info", timestamp, "lingotok")
    print (ori_str)
    signature_256 = sha256_encrypt(ori_str)
    url = "https://api.lingotok.ai/api/v1/video/create_video_info"
    response = requests.post(url, json=req, headers={"Content-Type": "application/json", "Timestamp": timestamp, "Signature": signature_256})
    # print (response.json()["video_id"])
    return response.json()

def convert_subtitles(zh_srt, en_srt, ar_srt, pinyin_srt):
    sub_zh = {"language": "zh", "obs_object": zh_srt.split("/")[-1]}
    sub_en = {"language": "en", "obs_object": en_srt.split("/")[-1]}
    sub_ar = {"language": "ar", "obs_object": ar_srt.split("/")[-1]}
    sub_py = {"language": "pinyin", "obs_object": pinyin_srt.split("/")[-1]}
    return [sub_zh, sub_en, sub_ar, sub_py]

def convert_quiz(quiz):
    def find_option(option_list, answer):
        if answer.strip()[0] == "A" or answer == option_list[0]:
            return option_list[0]
        elif answer.strip()[0] == "B" or answer == option_list[1]:
            return option_list[1]
        elif answer.strip()[0] == "C" or answer == option_list[2]:
            return option_list[2]
        elif answer.strip()[0] == "D" or answer == option_list[3]:
            return option_list[3]
        print ("find answer error")
        return option_list[1]
        
    quiz_zh = {"language": "zh", "question": quiz["question"], "option_list": quiz["options"], "answer_list": [find_option(quiz["options"], quiz["answer"])], "explanation": quiz["explanation"]}
    quiz_en = {"language": "en", "question": quiz["en_question"], "option_list": quiz["en_options"], "answer_list": [find_option(quiz["en_options"], quiz["answer"])], "explanation": quiz["en_explanation"]}
    quiz_ar = {"language": "ar", "question": quiz["ar_question"], "option_list": quiz["ar_options"], "answer_list": [find_option(quiz["ar_options"], quiz["answer"])], "explanation": quiz["ar_explanation"]}
    quiz_out = {"quiz_id": quiz["vid"], "quiz_type": "single_choice", "quiz_language_list": [quiz_zh, quiz_en, quiz_ar]}
    return quiz_out

def create_with_csv(meta_file, csv_file, out_csv_file, customize=None, series_name=None):
    lines = open(meta_file).readlines()
    quizd = dict()
    for l in lines:
        data = json.loads(l.strip())
        quizd[str(data["vid"])] = data
    df = pd.read_csv(csv_file)
    columns = df.columns.to_list()
    has_video_id = False
    if "video_id" not in columns:
        columns.append("video_id")
    else:
        has_video_id  = True
        for idx, col in enumerate(columns):
            if col == "video_id":
                videoid_idx = idx

    df_list = df.values.tolist()
    import pdb;pdb.set_trace()

    for i in tqdm(range(df.shape[0])):
        if has_video_id:
            if df.iloc[i][videoid_idx] != "nan":
                continue
        video_path = df.iloc[i]["FileName"]
        title = "{}_{}".format(video_path.split("/")[-2], video_path.split("/")[-1].split(".")[0]).replace("_modified", "")
        if "quiz_id" in df.columns:
            vid = str(df.iloc[i]["quiz_id"])
        elif "VID" in df.columns:
            vid = str(df.iloc[i]["VID"])
        else:
            vid = df.iloc[i]["zh_srt"].split("/")[-1].split("_")[0].strip()
        if not vid in quizd.keys():
            print ("{} not in quizd".format(vid))
            continue
        zh_srt = df.iloc[i]["zh_srt"]
        en_srt = df.iloc[i]["en_srt"]
        ar_srt = df.iloc[i]["ar_srt"]
        pinyin_srt = df.iloc[i]["pinyin_srt"]
        subtitles = convert_subtitles(zh_srt, en_srt, ar_srt, pinyin_srt)
        
        quiz = convert_quiz(quizd[vid])
        dur = int(df.iloc[i]["audio_dur"] * 1000)
        level = df.iloc[i]["level"]
        asset_id = df.iloc[i]["asset_id"]
        audio_ratio = df.iloc[i]["audio_ratio"]
        try:
            # resp = create_video_internal(asset_id, title, dur, [quiz], subtitles, level, audio_ratio, customize=customize)
            resp = create_video_internal_new(asset_id, title, dur, [quiz], subtitles, level, audio_ratio, customize=customize, series_name=series_name)
            if resp["code"] == 200 and resp["message"] == "success":
                if has_video_id:
                    df_list[i][videoid_idx] = resp["data"]["video_info"]["video_id"]
                else:
                    df_list[i].append(resp["data"]["video_info"]["video_id"])
            else:
                if has_video_id:
                    df_list[i][videoid_idx] = "nan"
                else:
                    df_list[i].append("nan")
                print ("Failed in {}".format(title))


        except Exception as e:
            if has_video_id:
                df_list[i][videoid_idx] = "nan"
            else:
                df_list[i].append("nan")
            print (e)
            print ("error in {}".format(title))
    
    df_out = pd.DataFrame(df_list, columns=columns)
    df_out.to_csv(out_csv_file, index=False)

def update_video_info(video_id, customize=None, series_name=None, level=None, tag_list=None):
    req = {"video_id": video_id}
    if customize is not None:
        req["customize"] = customize
    if tag_list is not None:
        req["tag_list"] = tag_list
    if series_name is not None:
        req["series_name"] = series_name
    if level is not None:
        req["level"] = level
    timestamp = str(int(time.time()))
    ori_str = "{}{}{}".format("update_video_info", timestamp, "lingotok")
    signature_256 = sha256_encrypt(ori_str)
    url = "https://api.lingotok.ai/api/v1/video/update_video_info"
    response = requests.post(url, json=req, headers={"Content-Type": "application/json", "Timestamp": timestamp, "Signature": signature_256})
    return response.json()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
def delete_video_info_by_video_id(video_id):
    req = {"video_id": video_id}
    timestamp = str(int(time.time()))
    ori_str = "{}{}{}".format("delete_video_info", timestamp, "lingotok")
    signature_256 = sha256_encrypt(ori_str)
    url = "https://api.lingotok.ai/api/v1/video/delete_video_info"
    response = requests.post(url, json=req, headers={"Content-Type": "application/json", "Timestamp": timestamp, "Signature": signature_256})
    return response.json()

def delete_videos_info_by_series_name(series_name):
    db = mongo_client["lingotok"]
    collection = db["video"]
    videos = collection.find({"series_name": series_name, "deleted": False})
    
    # 收集所有视频ID
    video_ids = [str(video["_id"]) for video in videos]

    # 使用线程池并行处理删除操作
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 创建future到video_id的映射
        future_to_id = {
            executor.submit(delete_video_info_by_video_id, video_id): video_id 
            for video_id in video_ids
        }
        
        # 处理完成的任务
        for future in tqdm(as_completed(future_to_id), total=len(video_ids)):
            video_id = future_to_id[future]
            try:
                result = future.result()
                if result["code"] != 200:
                    print(f"删除视频 {video_id} 失败: {result}")
            except Exception as e:
                print(f"处理视频 {video_id} 时发生错误: {e}")
    # clean_series_name_from_redis(series_name)

def clean_series_name_from_redis(series_name):
    yepzan_redis = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")
    # keys = yepzan_redis.keys()
    # level_interests = list()
    # for key in keys:
    #     if key.decode('utf-8').find("video_series_interest_level") != -1:
    #         level_interests.append(key.decode('utf-8'))
    level_interests = ['video_series_interest_level-人生建议_难', 'video_series_interest_level-vlog_难', 'video_series_interest_level-美妆/穿搭_难', 'video_series_interest_level-小品/脱口秀/相声_难', 'video_series_interest_level-艺术_难', 'video_series_interest_level-食物/饮料_难', 'video_series_interest_level-汽车_难', 'video_series_interest_level-宠物/动物_难', 'video_series_interest_level-科学教育_中等', 'video_series_interest_level-生活小窍门_难', 'video_series_interest_level-动漫漫画_难', 'video_series_interest_level-解压/助眠/令人满足的（ASMR/刮肥皂/太空沙/液压机等等）_难', 'video_series_interest_level-vlog_中等', 'video_series_interest_level-DIY_难', 'video_series_interest_level-旅行_难', 'video_series_interest_level-宠物/动物_中等', 'video_series_interest_level-艺术_中等', 'video_series_interest_level-舞蹈_难', 'video_series_interest_level-科学教育_难', 'video_series_interest_level-家庭_中等', 'video_series_interest_level-音乐_中等', 'video_series_interest_level-科学教育_入门', 'video_series_interest_level-科学教育_初学', 'video_series_interest_level-娱乐_中等', 'video_series_interest_level-娱乐_难', 'video_series_interest_level-动漫漫画_中等', 'video_series_interest_level-运动_难', 'video_series_interest_level-舞蹈_中等', 'video_series_interest_level-家庭_难']
    
    yepzan_redis.delete("video_series-{}".format(series_name))
    for level in ["入门", "初学", "中等", "难"]:
        key = "video_series_level-{}".format(level)
        raw_list = yepzan_redis.lrange(key, 0, -1)
        series_list = [item.decode('utf-8') for item in raw_list]
        if series_name in series_list:
            series_list.remove(series_name)
            series_list = list(set(series_list))
            yepzan_redis.delete(key)
            if len(series_list) > 0:
                yepzan_redis.rpush(key, *series_list)
                print (key)
                print ([item.decode('utf-8') for item in yepzan_redis.lrange(key, 0, -1)])
                for level_interest in level_interests:
                    if level_interest.find(level) != -1:
                        level_interest_series_names = yepzan_redis.lrange(level_interest, 0, -1)
                        level_interest_series_names = [item.decode('utf-8') for item in level_interest_series_names]
                        if series_name in level_interest_series_names:
                            level_interest_series_names.remove(series_name)
                            level_interest_series_names = list(set(level_interest_series_names))
                            yepzan_redis.delete(level_interest)
                            if len(level_interest_series_names) > 0:
                                yepzan_redis.rpush(level_interest, *level_interest_series_names)
                                print (level_interest)
                                print ([item.decode('utf-8') for item in yepzan_redis.lrange(level_interest, 0, -1)])

def clean_redis_with_baimingdan(baimingdan):
    yepzan_redis = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")
    for level in ["入门", "初学", "中等", "难"]:
        key = "video_series_level-{}".format(level)
        raw_list = yepzan_redis.lrange(key, 0, -1)
        raw_list = [item.decode('utf-8') for item in raw_list]
        left_list = []
        for item in raw_list:
            if item in baimingdan:
                left_list.append(item)
        yepzan_redis.delete(key)
        if len(left_list) > 0:
            yepzan_redis.rpush(key, *left_list)
            print (key)
            print ([item.decode('utf-8') for item in yepzan_redis.lrange(key, 0, -1)])
    
    level_interests = ['video_series_interest_level-人生建议_难', 'video_series_interest_level-vlog_难', 'video_series_interest_level-美妆/穿搭_难', 'video_series_interest_level-小品/脱口秀/相声_难', 'video_series_interest_level-艺术_难', 'video_series_interest_level-食物/饮料_难', 'video_series_interest_level-汽车_难', 'video_series_interest_level-宠物/动物_难', 'video_series_interest_level-科学教育_中等', 'video_series_interest_level-生活小窍门_难', 'video_series_interest_level-动漫漫画_难', 'video_series_interest_level-解压/助眠/令人满足的（ASMR/刮肥皂/太空沙/液压机等等）_难', 'video_series_interest_level-vlog_中等', 'video_series_interest_level-DIY_难', 'video_series_interest_level-旅行_难', 'video_series_interest_level-宠物/动物_中等', 'video_series_interest_level-艺术_中等', 'video_series_interest_level-舞蹈_难', 'video_series_interest_level-科学教育_难', 'video_series_interest_level-家庭_中等', 'video_series_interest_level-音乐_中等', 'video_series_interest_level-科学教育_入门', 'video_series_interest_level-科学教育_初学', 'video_series_interest_level-娱乐_中等', 'video_series_interest_level-娱乐_难', 'video_series_interest_level-动漫漫画_中等', 'video_series_interest_level-运动_难', 'video_series_interest_level-舞蹈_中等', 'video_series_interest_level-家庭_难']
    for key in level_interests:
        raw_list = yepzan_redis.lrange(key, 0, -1)
        raw_list = [item.decode('utf-8') for item in raw_list]
        left_list = []
        for item in raw_list:
            if item in baimingdan:
                left_list.append(item)
        yepzan_redis.delete(key)
        if len(left_list) > 0:
            yepzan_redis.rpush(key, *left_list)
            print (key)
            print ([item.decode('utf-8') for item in yepzan_redis.lrange(key, 0, -1)])
    
    print (level_interests)


def resue_with_csv(create_csv, new_code):
    df = pd.read_csv(create_csv)
    from create_video import update_video_info
    for i in tqdm(range(df.shape[0])):
        video_id = df.iloc[i]["video_id"]
        update_video_info(video_id, customize=new_code)

if __name__ == "__main__":
    # pass
    # delete_videos_info_by_series_name("PNU_2")
    yepzan_redis = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")
    # raw_list = yepzan_redis.lrange("video_series_interest_level-宠物/动物_中等", 0, -1)
    # raw_list = [item.decode('utf-8') for item in raw_list]
    # print (raw_list)
    # for level in ["入门", "初学", "中等", "难"]:
    #     key = "video_series_level-{}".format(level)
    
    # for level in ["easy", "medium", "hard"]:
    #     key = "series_level-{}".format(level)
    #     print(key)
    #     raw_list = yepzan_redis.lrange(key, 0, -1)
    #     raw_list = [item.decode('utf-8') for item in raw_list]
    #     print (raw_list)
    
    # print (json.loads(yepzan_redis.get("series-68297be2493ae9d4e77ed4ac")))

    print (yepzan_redis.lrange("series_video-68297be2493ae9d4e77ed4ac", 0, 5))
    
    
    # update_video_info("681b952f670e05b9259ff6c0", series_name="2024版新教材拼音朗读视频全集")
    # pass
    # update_video_info("6799bd383f4ca0bb2aa30bda", "PNU888 ")
    # resue_with_csv("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0126/create.csv", "PNU888")
    # update_videoinfo_recommender_withcsv("/Users/tal/work/lingtok_server/video_process/hw/videos/车/陈hh/video_info.csv")
    # create_with_csv("../video_metainfo.jsonl", "/Users/tal/work/lingtok_server/video_process/hw/video_info_800_uploaded.csv", "/Users/tal/work/lingtok_server/video_process/hw/video_info_800_created.csv")
    # print (json.dumps(convert_quiz(quiz), ensure_ascii=False))
    # print (update_video_info("67adb527f2eb187c98ad619b", tag_list=["娱乐", "家庭"]))
    # print (delete_video _info("679691d7cff591f86bb2195a"))
    # delete_videos_info_by_series_name("0120-汉字基本偏旁-04-01-君-干锅鱼已修改")
    # delete_videos_info_by_series_name("悟空识字")
    # delete_videos_info_by_series_name("【已标总表】01 11-超好用的认字动画-34-01-创逸星")
    # delete_videos_info_by_series_name("Maggunur带你学国语压缩包-君_new")
    # delete_videos_info_by_series_name("0112-汉字基本偏旁-04-01-干锅鱼")
    # delete_videos_info_by_series_name("超级宝贝JOJO】1-5季 中文儿歌")
    # delete_videos_info_by_series_name("PNU_1")
    # delete_videos_info_by_series_name("PNU_2")
    # clean_series_name_from_redis("PNU_1")
    # clean_series_name_from_redis("PNU_2")
    # clean_series_name_from_redis("【已标总表】01 11-超好用的认字动画-34-01-创逸星")
    # clean_series_name_from_redis("0120-汉字基本偏旁-04-01-君-干锅鱼已修改")
    # clean_series_name_from_redis("Maggunur带你学国语压缩包-君_new")
    # clean_series_name_from_redis("0112-汉字基本偏旁-04-01-干锅鱼")
    # clean_series_name_from_redis("超级宝贝JOJO】1-5季 中文儿歌")
    # delete_videos_info_by_series_name("Maggunur带你学国语")
    # delete_videos_info_by_series_name("HSK output")
    # delete_videos_info_by_series_name("HSK output_new_2 20250107")
    # clean_series_name_from_redis("HSK output_new_2 20250107")
    # delete_videos_info_by_series_name("维C动物园的作品")
    # a = {'', '维C动物园的作品', '【已标总表】yt-0110-8267-主妇千金回忆录（59集）-君-成-修改后', '毕的二阶导', '9901-我的玄主身份藏不住了（80集）于宙童', '陈hh', '111', '晴珍', '长篇', '-0111-阎鹤祥-50-02-salience', '小Lin说_NEW_君_4', '9905-崛起，从掌控时间开始（88集）李若希&杨泽琳', '【已标总表】01 11 开饭了大熊猫part2-创逸星(2-2)', '何同学', '9903-顾总的签约小情人（80集）', 'TEST', '汉字动画课程-30-04_君_成', 'testbaac', '0108-开饭了大熊猫-part- salience', '新东方比邻国际中文', '小Lin说', '精品资源坊经典儿歌'}
    # for series_name in tqdm(list(a)):
    #     delete_videos_info_by_series_name(series_name)

    # baimingdan = ["开饭了大熊猫-part", "开饭了大熊猫part2", "0121-画渣花小烙-118-01-salience", "程十安an", "湖南卫视你好星期六_NEW181", "开饭了大熊猫", "0120-恋与深空-72-01-salience", "0120-王泡芙的抖音", "0122-黑猫少女Bella-20-01-salience", "不刷题的吴姥姥", "航拍中国", "舌尖", "苏苏家的三小只", "何同学", "房琪kiki-43个", "再见爱人-71个有效", "妞妞 NCC07208682 中国旅游", "妞妞 Lalla香香 2025年1月5日", "黑麒麟点评_NEW83"]
    # yepzan_redis = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")
    # for level in ["入门", "初学"]:
    #     key = "video_series_level-{}".format(level)
    #     raw_list = yepzan_redis.lrange(key, 0, -1)
    #     series_list = [item.decode('utf-8') for item in raw_list]
    #     baimingdan += series_list
    
    # print (baimingdan)
    # baimingdan = ['开饭了大熊猫-part', '开饭了大熊猫part2', '0121-画渣花小烙-118-01-salience', '程十安an', '湖南卫视你好星期六_NEW181', '开饭了大熊猫', '0120-恋与深空-72-01-salience', '0120-王泡芙的抖音', '0122-黑猫少女Bella-20-01-salience', '不刷题的吴姥姥', '航拍中国', '舌尖', '苏苏家的三小只', '何同学', '房琪kiki-43个', '再见爱人-71个有效', '妞妞 NCC07208682 中国旅游', '妞妞 Lalla香香 2025年1月5日', '黑麒麟点评_NEW83', 'hsk_自制', '悟空识字', '被偷走爱的那十年', '我被套路撞了N下腰', '拼音规则', '声母歌', '声母练习', 'HSK_1_2_3_写字视频', '初级口语风景单词卡', '2024版新教材拼音朗读视频全集', '悟空识字', 'HSK_表情包', '字有道理']

    # clean_redis_with_baimingdan(baimingdan)
    # delete_videos_info_by_series_name("pnu1")
    # delete_videos_info_by_series_name("pnu2")


    # print (baimingdan)

    # df = pd.read_csv("/Users/tal/work/lingtok_server/analysis/video_infos_20250425_with_urls.csv")
    # series_names = df["series_name"].unique()
    # start_idx = 500
    # end_idx = 1000
    # print ([item for item in series_names[start_idx:end_idx] if item not in baimingdan])
    # import pdb;pdb.set_trace()
    # for series_name in tqdm(series_names[start_idx:end_idx]):
        
    #     if series_name in baimingdan:
    #         continue
    #     else:
    #         try:
    #             print (series_name)
    #             delete_videos_info_by_series_name(series_name)
    #         except Exception as e:
    #             print (e)
    #             print ("error in {}".format(series_name))

    
