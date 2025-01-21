import requests
import json
import pandas as pd
from tqdm import tqdm

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

def convert_subtitles(zh_srt, en_srt, ar_srt, pinyin_srt):
    sub_zh = {"language": "zh", "obs_object": zh_srt.split("/")[-1]}
    sub_en = {"language": "en", "obs_object": en_srt.split("/")[-1]}
    sub_ar = {"language": "ar", "obs_object": ar_srt.split("/")[-1]}
    sub_py = {"language": "pinyin", "obs_object": pinyin_srt.split("/")[-1]}
    return [sub_zh, sub_en, sub_ar, sub_py]

def convert_quiz(quiz):
    def find_option(option_list, answer):
        if answer.strip()[0] == "A":
            return option_list[0]
        elif answer.strip()[0] == "B":
            return option_list[1]
        elif answer.strip()[0] == "C":
            return option_list[2]
        elif answer.strip()[0] == "D":
            return option_list[3]
        print ("find answer error")
        return option_list[1]
        
    quiz_zh = {"language": "zh", "question": quiz["question"], "option_list": quiz["options"], "answer_list": [find_option(quiz["options"], quiz["answer"])], "explanation": quiz["explanation"]}
    quiz_en = {"language": "en", "question": quiz["en_question"], "option_list": quiz["en_options"], "answer_list": [find_option(quiz["en_options"], quiz["answer"])], "explanation": quiz["en_explanation"]}
    quiz_ar = {"language": "ar", "question": quiz["ar_question"], "option_list": quiz["ar_options"], "answer_list": [find_option(quiz["ar_options"], quiz["answer"])], "explanation": quiz["ar_explanation"]}
    quiz_out = {"quiz_id": quiz["vid"], "quiz_type": "single_choice", "quiz_language_list": [quiz_zh, quiz_en, quiz_ar]}
    return quiz_out

def create_with_csv(meta_file, csv_file, out_csv_file, customize=None):
    lines = open(meta_file).readlines()
    quizd = dict()
    for l in lines:
        data = json.loads(l.strip())
        quizd[data["vid"]] = data
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
    for i in tqdm(range(df.shape[0])):
        if has_video_id:
            if df.iloc[i][videoid_idx] != "nan":
                continue
        video_path = df.iloc[i]["FileName"]
        title = "{}_{}".format(video_path.split("/")[-2], video_path.split("/")[-1].split(".")[0])
        if "VID" in df.columns:
            vid = df.iloc[i]["VID"]
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
        import pdb;pdb.set_trace()
        quiz = convert_quiz(quizd[vid])
        dur = int(df.iloc[i]["audio_dur"] * 1000)
        level = df.iloc[i]["level"]
        asset_id = df.iloc[i]["asset_id"]
        audio_ratio = df.iloc[i]["audio_ratio"]
        
        try:
            resp = create_video_internal(asset_id, title, dur, [quiz], subtitles, level, audio_ratio, customize=customize)
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


if __name__ == "__main__":
    update_videoinfo_recommender_withcsv("/Users/tal/work/lingtok_server/video_process/hw/videos/车/陈hh/video_info.csv")
    # create_with_csv("../video_metainfo.jsonl", "/Users/tal/work/lingtok_server/video_process/hw/video_info_800_uploaded.csv", "/Users/tal/work/lingtok_server/video_process/hw/video_info_800_created.csv")
    # print (json.dumps(convert_quiz(quiz), ensure_ascii=False))

