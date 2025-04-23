import os
import sys
import pandas as pd
import shutil
import json
from video_processor import VideoProcessor, translate_quiz_metainfo
from tqdm import tqdm
from vod_hw_util import upload_hw_withcsv
import random as rd
# from content_tagger import tag_video_info_csv_audio_ratio

import uuid
from create_video import create_with_csv, update_videoinfo_recommender_withcsv
from content_tagger import update_video_info_csv_level

os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from moviepy.editor import VideoFileClip, TextClip, CompositeVideoClip
from moviepy.config import change_settings
change_settings({"IMAGEMAGICK_BINARY": "/opt/homebrew/Cellar/imagemagick/7.1.1-43/bin/magick"})
from pypinyin import pinyin
from retrying import retry

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from recommender.quiz_generator import CharPinyinQuizGeneratingWorker, QuizGeneratingCtx

def add_pinyin_to_video(video_path, word, out_video_file):
    video = VideoFileClip(video_path)
    py_list = pinyin(word)
    py_str = " ".join(py_list[0])
    txt_clip_chinese = TextClip("{}\n{}".format(py_str, word), fontsize=60, color='black', font='Songti-SC-Black')
    txt_clip_chinese = txt_clip_chinese.set_position(('center', 0.15), relative=True).set_duration(video.duration)
    video = CompositeVideoClip([video, txt_clip_chinese])
    video.write_videofile(out_video_file, codec='libx264', audio_codec='aac')


def update_quiz_jsonl_withcsv(ori_file, new_file):
    df = pd.read_csv("210 quiz-summary.csv")
    quizd = dict()
    for i in range(df.shape[0]):
        is_fixed = str(df.iloc[i][0]).strip()
        if is_fixed == "1":
            q_opt = df.iloc[i][5]
            q = q_opt.split("\n")[0].replace("Q:", "").strip()
            ans = df.iloc[i][6]
            options = [item.strip() for item in q_opt.replace("Options:", "").split("\n")[1:]]
            vid = df.iloc[i][7].split(".")[0].strip().split("_")[0].strip()

            quizd[vid] = {"question": q, "options": options, "answer": ans}

    lines = open(ori_file).readlines()
    fw = open(new_file, "w")
    for l in lines:
        item = json.loads(l.strip())
        vid = item["vid"]
        if vid in quizd.keys():
            item["question"] = quizd[vid]["question"]
            item["options"] = quizd[vid]["options"]
            item["answer"] = quizd[vid]["answer"]
            print ("update")

        fw.write(json.dumps(item) + "\n")
    fw.close()

def generate_quiz(ensrt_dir, metainfo_file):
    fw = open(metainfo_file, "w")
    video_processor = VideoProcessor()
    for root, dirs, files in os.walk(ensrt_dir):
        for f in files:
            if f.find("English.srt") == -1:
                continue
            try:
                ensrt_filename = os.path.join(root, f.replace("\\", "/"))
                vid = f.split("_")[0]
                video_processor.load_srt(ensrt_filename)
                quiz = video_processor.generate_quiz()
                quiz["vid"] = vid
                # print (quiz)
                fw.write(json.dumps(quiz) + "\n")
            except Exception as e:
                print (str(e))
    fw.close()

def generate_quiz_zh(ensrt_dir, metainfo_file):
    fw = open(metainfo_file, "w", encoding="utf-8")
    video_processor = VideoProcessor()
    for root, dirs, files in os.walk(ensrt_dir):
        for f in tqdm(files):
            if f.find("Chinese.srt") == -1:
                continue
            try:
                zhsrt_filename = os.path.join(root, f.replace("\\", "/"))
                vid = f.split("_")[0]
                video_processor.load_srt(zhsrt_filename)
                quiz = video_processor.generate_quiz_zh_tiankong_v2(zhsrt_filename)
                if quiz == None:
                    continue
                os.system("sleep 1")
                quiz["vid"] = vid
                # print (quiz)
                fw.write(json.dumps(quiz, ensure_ascii=False) + "\n")
            except Exception as e:
                print (str(e))
    fw.close()

@retry(stop_max_attempt_number=3)
def generate_sent_quiz(word,py):
    from llm_util import call_doubao_pro_32k
    json_str = json.dumps({"question": "“你”的拼音是什么？", "options": ["wǒ", "nǐ", "mǐ", "níng"], "answer": "nǐ", "explanation": "“你”的拼音是nǐ。"})
    prompt = "“{}”的拼音是{}，将其变成一个四选一的题目，题干是:“{}”的拼音是什么？,选项为包含正确拼音在内的四个拼音，注意错误的选项需要改变声韵母中的至少一个, 结果用Json格式进行返回，json格式如下{}。".format(word, py, word, json_str)
    resp = call_doubao_pro_32k(prompt)
    content_str = resp.replace("```json", "").replace("```", "")
    content = json.loads(content_str)
    print (content)
    return content

def prep_srt_data():
    df = pd.read_csv("video_info.csv")
    # zh_srt_list = list()
    ar_srt_list = list()
    video_processor = VideoProcessor()
    for i in tqdm(range(df.shape[0])):
        try:
            tmp_srt = df.iloc[i]["en_srt"]
            res = video_processor.translate_srt(tmp_srt)
            ar_srt_list.append(res["ar_srt"].replace("/", "\\"))
        except:
            print ("bad data")
            ar_srt_list.append("")
    df["ar_srt"] = ar_srt_list
    df.to_csv("video_info_withar.csv")
    
def prep_hw_data():

    ## 等待merge
    video_dir_list = []
    video_dir_list.append("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200")


    for video_dir in video_dir_list:
        video_list = list()
        for root, dirs, files in os.walk(video_dir):
            for f in files:
                if f.find(".mp4") != -1 and f.find("modified") == -1:
                    modified_video_path = os.path.join(root, f.replace(".mp4", "_modified.mp4"))
                    if not os.path.exists(modified_video_path):
                        add_pinyin_to_video(os.path.join(root, f), f.split(".")[0], modified_video_path)
                    video_list.append(modified_video_path)

        srt_dir = os.path.join(video_dir, "srt_dir")
        out_csv_file = os.path.join(video_dir, "video_info.csv")
        srt_csv_file = os.path.join(video_dir, "video_info_srt.csv")
        # chunk_csv_file = os.path.join(video_dir, "video_info_chunk.csv")

        # quiz_zh_metainfo_file = os.path.join(video_dir, "video_metainfo_zh.jsonl")
        quiz_metainfo_file = os.path.join(video_dir, "video_metainfo.jsonl")

        quiz_csv_file = os.path.join(video_dir, "video_info_quiz.csv")
        # compressed_csv_file = os.path.join(video_dir, "video_info_compressed.csv")
        vod_csv_file = os.path.join(video_dir, "video_info_vod_hw.csv")
        tag_csv_file = os.path.join(video_dir, "video_info_tag.csv")
        cus_tag = "PNU888"
        
        # For debug
        skip_srt = True
        skip_quiz = True
        skip_tag_video = True
        # skip_compress = True
        skip_upload = True

        skip_create = False
        skip_series_name = False

        video_processor = VideoProcessor()

        if not skip_srt:
        
            if not os.path.exists(srt_dir):
                os.makedirs(srt_dir)

            columns = ["FileName", "title", "单词", "zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
            df_list = list()
            for i in tqdm(range(len(video_list))):
                item = list()
                video_path = video_list[i]
                srt_name = str(uuid.uuid4())
                item.append(video_path)
                title = "{}_{}".format(video_path.split("/")[-2], video_path.split("/")[-1].split(".")[0])
                item.append(title)
                word = video_list[i].split("/")[-1].split(".")[0]
                item.append(word)

                os.system("/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg -y -loglevel error -i \"{}\" -ac 1 -ar 16000 -f wav test.wav".format(video_path))
                srt_res = video_processor.generate_zhsrt("",  os.path.join(srt_dir, srt_name), audio_path="test.wav", gen_ar=True)
                os.system("rm test.wav")
                if srt_res == None:
                    continue
                else:
                    item.append(srt_res["zh_srt"])
                    item.append(srt_res["ar_srt"])
                    item.append(srt_res["en_srt"])
                    item.append(srt_res["pinyin_srt"])
                df_list.append(item)
            df_srt = pd.DataFrame(df_list, columns=columns)
            df_srt.to_csv(srt_csv_file, index=False)
        
        
        df = pd.read_csv(srt_csv_file)
        if not skip_quiz:
            columns = df.columns.to_list()
            columns.append("quiz_id")
            columns.append("拼音")
            df_list = df.values.tolist()
            fw = open(quiz_metainfo_file, "w", encoding="utf-8")

            quiz_ctx = QuizGeneratingCtx()
            quiz_worker_config = {"hsk_zh_en_ar_path": "hsk_dictionary/HSK_zh_en_ar.csv", "hsk_char_path": "hsk_dictionary/HSK_char.csv"}
            quiz_worker = CharPinyinQuizGeneratingWorker(quiz_worker_config)
            for i in tqdm(range(df.shape[0])):
                word = df.iloc[i]["title"].split("_")[-2].strip()
                df_list[i][1] = df_list[i][1].replace("_modified", "")
                py = pinyin(word)[0][0]
                quiz_ctx.extracted_word = word
                try:
                    quiz_res = quiz_worker.action(quiz_ctx)
                
                
                    content = dict()
                    content["question"] = quiz_res["quiz_language_list"][0]["question"]
                    content["options"] = quiz_res["quiz_language_list"][0]["option_list"]
                    content["answer"] = quiz_res["quiz_language_list"][0]["answer_list"][0]
                    content["explanation"] = ""
                    
                    # multi_lingual_quiz = video_processor.translate_zh_quiz(content)
                    content["ar_question"] = quiz_res["quiz_language_list"][1]["question"]
                    content["ar_options"] = quiz_res["quiz_language_list"][1]["option_list"]
                    content["ar_explanation"] = ""
                    content["en_question"] = quiz_res["quiz_language_list"][2]["question"]
                    content["en_options"] = quiz_res["quiz_language_list"][2]["option_list"]
                    content["en_explanation"] = ""
                    content["vid"] = "{}_{}".format(i, word)
                    df_list[i].append(content["vid"])
                    df_list[i].append(py)
                    fw.write("{}\n".format(json.dumps(content, ensure_ascii=False)))
                except Exception as e:
                    print (str(e))
                
            fw.close()
            df_list = pd.DataFrame(df_list, columns=columns)
            df_list.to_csv(quiz_csv_file, index=False)
        
        if not skip_tag_video:
            update_video_info_csv_level(quiz_csv_file, tag_csv_file)

        if not skip_upload:
            if os.path.exists(vod_csv_file):
                upload_hw_withcsv(vod_csv_file, vod_csv_file)
            else:
                upload_hw_withcsv(tag_csv_file, vod_csv_file)
            
            df_vod = pd.read_csv(vod_csv_file)

            null_num = df_vod["asset_id"].isnull().sum()
            all_num = df_vod.shape[0]
            while null_num > int(0.05 * all_num):
                upload_hw_withcsv(vod_csv_file, vod_csv_file)
                df_vod = pd.read_csv(vod_csv_file)
                null_num = df_vod["asset_id"].isnull().sum()
                all_num = df_vod.shape[0]

            # upload_huoshan_withcsv(tag_csv_file, vod_csv_file)

        if not skip_create:
            create_with_csv(quiz_metainfo_file, vod_csv_file, out_csv_file, customize=cus_tag, series_name="悟空识字")
        
        if not skip_series_name:
            from recommender.video_updater import VideoUpdater
            video_updater = VideoUpdater()
            video_updater.update_series_tag_once("悟空识字", level="初学", tag_list=["科学教育"])
    
        # merge_csv_huoshan("/Users/tal/work/lingtok_server/video_info_hw_created.csv", out_csv_file)

        # generate_quiz_zh(srt_dir, os.path.join(video_dir, "video_metainfo_zhonly.jsonl"))
        # merge_csv_huoshan("video_info_huoshan.csv", srt_csv_file, os.path.join(video_dir, "video_metainfo_zhonly.jsonl"))
        # os.system("scp  {}/*.srt root@54.248.147.60:/dev/data/lingotok_server/huoshan/srt_dir".format(srt_dir))
        # translate_quiz_metainfo(os.path.join(video_dir, "video_metainfo_zhonly.jsonl"), os.path.join(video_dir, "video_metainfo.jsonl"))
        # os.system("cat {} >> ../video_metainfo.jsonl".format(os.path.join(video_dir, "video_metainfo.jsonl")))
        # tag_video_info_csv_audio_ratio("video_info_huoshan.csv", "../video_info_huoshan.csv")

if __name__ == '__main__':
    prep_hw_data()
    # prep_aigc_huoshan()
    # prep_huoshan_data()
    # df = pd.read_csv("video_info_530.csv")
    # cp_esrt(df, "video_Finished_361_525_ori", "video_Finished_361_525_ensrt", minid=361, maxid=525)
    
    # update_quiz_jsonl_withcsv("video_metainfo.jsonl", "video_metainfo_new.jsonl")
    # df = pd.read_csv("video_info_0909.csv")
    # cp_video_ce(df, "video_database")
    # df = pd.read_csv("video_info_0914.csv")
    # cp_video_ce(df, "video_database_0914_videoce", minid=71)
    # prep_video_quiz("Generated_Questions_0918", "video_metainfo.jsonl")
    # update_video_info_csv("../lingtok_server/video_info.csv", "../lingtok_server/video_info_refine.csv", log_csv_filename="210_reason_tag.csv")
    # update_video_info_csv("../lingtok_server/video_info.csv", "../lingtok_server/video_info_refine_14B.csv", log_csv_filename="210_reason_tag_14B.csv")
    # update_video_info_csv("../lingtok_server/video_info.csv", "../lingtok_server/video_info_refine_14B_3shot.csv", log_csv_filename="210_reason_tag_14B_3shot.csv")
    # generate_quiz("video_Finished_361_525_ensrt", "video_metainfo_361_525.jsonl")
    # merge_csv("../lingtok_server/video_info.csv", "video_info_530.csv", "/Users/tal/work/lingtok/lingtok_server/video_metainfo.jsonl", "video_info_merged.csv")

    # df = pd.read_csv("video_info_530.csv")
    # cp_esrt(df, "video_Finished_1_377_ori", "video_Finished_211_360_ensrt", minid=211, maxid=360)
    # generate_quiz("video_Finished_211_360_ensrt", "video_metainfo_211_360.jsonl")
    # merge_csv("../lingtok_server/video_info.csv", "video_info_530.csv", "/Users/tal/work/lingtok/lingtok_server/video_metainfo.jsonl", "video_info_merged_1_530.csv")
    # update_video_info_csv("video_info_merged_1_530.csv", "video_info_merged_1_530_relevel.csv", log_csv_filename="530_7b_reason_tag.csv", minid=211, maxid=600)

    # prep_tangzong_data()
    # update_video_info_csv("tangzong_video_info.csv", "tangzong_video_info_level.csv")
    # generate_quiz("Video_Finished", "tangzong_video_metainfo.jsonl")
    # merge_csv("../video_info.csv", "tangzong_video_info_level.csv", "/Users/tal/work/lingtok/lingtok_server/video_metainfo.jsonl", "video_info_merged_tangzong.csv")

    # prep_zhongdong_data()

    # prep_srt_data()


