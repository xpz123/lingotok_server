import pandas as pd
import os
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from translator import translate_text2ar
from pypinyin import pinyin

from moviepy.editor import VideoFileClip, AudioFileClip, TextClip, CompositeVideoClip, ColorClip, concatenate_videoclips, CompositeAudioClip, ImageSequenceClip
from moviepy.config import change_settings
change_settings({"IMAGEMAGICK_BINARY": "/opt/homebrew/Cellar/imagemagick/7.1.1-43/bin/magick"})
from bidi.algorithm import get_display
import arabic_reshaper
from llm_util import call_doubao_pro_32k
from huoshan_tts_util import generate_wav

from tqdm import tqdm

import subprocess
import re
import json
from retrying import retry
from video_processor import VideoProcessor, milliseconds_to_time_string
import time
from aigc import merge_audios
import random as rd

def trans_word_to_ar(ori_csv_file, ar_csv_file):
    df = pd.read_csv(ori_csv_file)
    columns = df.columns.to_list()
    columns.append("阿语翻译")
    text_list = list()
    for i in tqdm(range(df.shape[0])):
        text = df.iloc[i]["单词"]
        text_list.append(text.split("（")[0])
    resp_list = translate_text2ar(text_list, "ar")
    assert len(text_list) == len(resp_list)
    ar_text_list = [resp["Translation"] for resp in resp_list]
    df["阿语翻译"] = ar_text_list
    df.to_csv(ar_csv_file, index=False)

def add_pinyin(ori_csv_file, py_csv_file):
    # import pdb;pdb.set_trace()
    df = pd.read_csv(ori_csv_file)
    df.dropna(subset=["单词"], axis=0, how="any", inplace=True)
    columns = df.columns.to_list()
    columns.append("拼音")
    df_list = df.values.tolist()
    for i in range(df.shape[0]):
        py_list = pinyin(df.iloc[i]["单词"])
        py_str = ""
        for item in py_list:
            for py in item:
                py_str += " " + py
        df_list[i].append(py_str)
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(py_csv_file, index=False)

def generate_subtitle(zh_text_list, srt_prefix, root_dir, word_dur):
    video_processor = VideoProcessor()
    res = dict()
    zh_srt = os.path.join(root_dir, "{}_Chinese.srt".format(srt_prefix))
    ar_srt = os.path.join(root_dir, "{}_Arabic.srt".format(srt_prefix))
    en_srt = os.path.join(root_dir, "{}_English.srt".format(srt_prefix))
    pinyin_srt = os.path.join(root_dir, "{}_Pinyin.srt".format(srt_prefix))

    res["zh_srt"] = zh_srt
    res["ar_srt"] = ar_srt
    res["en_srt"] = en_srt
    res["pinyin_srt"] = pinyin_srt
    
    ar_text_list = translate_text2ar(zh_text_list, "ar")
    assert len(ar_text_list) == len(zh_text_list)

    en_text_list = translate_text2ar(zh_text_list, "en")
    assert len(en_text_list) == len(zh_text_list)

    fw_zh = open(zh_srt, "w")
    fw_ar = open(ar_srt, "w")
    fw_en = open(en_srt, "w")
    
    start_time = 0
    for i in range(len(zh_text_list)):
        dur_ms = word_dur
        start_time_str = milliseconds_to_time_string(start_time)
        end_time_str = milliseconds_to_time_string(start_time + dur_ms)
        start_time = start_time + dur_ms
        zh_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{zh_text_list[i]}\n\n"
        fw_zh.write(zh_srt_content)

        ar_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{ar_text_list[i]['Translation']}\n\n"
        fw_ar.write(ar_srt_content)

        en_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{en_text_list[i]['Translation']}\n\n"
        fw_en.write(en_srt_content)
    fw_zh.close()
    fw_ar.close()
    fw_en.close()

    video_processor.convert_zhsrt_to_pinyinsrt(zh_srt, pinyin_srt)
    return res

@retry(stop_max_attempt_number=3)
def generate_sent_quiz(word,py):
    json_str = json.dumps({"question": "“你”的拼音是什么？", "options": ["wǒ", "nǐ", "mǐ", "níng"], "answer": "nǐ", "explanation": "“你”的拼音是nǐ。"})
    prompt = "“{}”的拼音是{}，将其变成一个四选一的题目，题干是:“{}”的拼音是什么？,选项为包含正确拼音在内的四个拼音，注意错误的选项需要改变声韵母中的至少一个, 结果用Json格式进行返回，json格式如下{}。".format(word, py, word, json_str)
    resp = call_doubao_pro_32k(prompt)
    content_str = resp.replace("```json", "").replace("```", "")
    content = json.loads(content_str)
    print (content)
    return content
        

def convert_video_to_csv(emoji_dir, out_csv):
    df_list = list()
    leagal_suffix = [".mp4"]
    for item in os.listdir(emoji_dir):
        is_legual = False
        for suffix in leagal_suffix:
            if item.endswith(suffix):
                is_legual = True
                break
        if not is_legual:
            continue
        word = item.split(".")[0]
        df_list.append([word, os.path.join(emoji_dir, item)])
    df = pd.DataFrame(df_list, columns=["单词", "FileName"])
    df.to_csv(out_csv, index=False)

def outdate_with_csv(create_csv, date):
    df = pd.read_csv(create_csv)
    from create_video import update_video_info
    for i in tqdm(range(df.shape[0])):
        video_id = df.iloc[i]["video_id"]
        update_video_info(video_id, customize="KAU777_{}".format(date))

if __name__ == "__main__":
    # df = pd.read_csv("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/词汇练习/words_refined.csv")
    # df = pd.read_excel("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1/words_chap1.xls")
    # df = pd.read_excel("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1/chap11/words_chap11.xls")
    
    video_processor = VideoProcessor()

    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0126"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0203"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0212"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0219"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0219_single_word"
    root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0326_single_word"
    word_dir = os.path.join(root_dir, "单字视频")
    # create_tag = "KAU777"
    create_tag = "PNU888"

    audio_dir = os.path.join(root_dir, "audios")
    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)
    video_dir = os.path.join(root_dir, "videos")
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    srt_dir = os.path.join(root_dir, "srt_dir")
    if not os.path.exists(srt_dir):
        os.makedirs(srt_dir)
    
    ori_csv = os.path.join(root_dir, "ori.csv")
    quiz_jsonl = os.path.join(root_dir, "words_quiz.jsonl")
    quiz_csv = os.path.join(root_dir, "quiz.csv")
    word_py_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin.csv")
    word_py_ar_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin_ar.csv")
    video_csv = os.path.join(root_dir, "video.csv")
    srt_csv = os.path.join(root_dir, "srt.csv")
    tag_csv = os.path.join(root_dir, "tag.csv")
    vod_csv = os.path.join(root_dir, "vod.csv")
    create_csv = os.path.join(root_dir, "create.csv")

    skip_py_arword = False
    skip_quiz = False
    skip_srt = False
    skip_tag = False
    skip_vod = True
    skip_create = False

    convert_video_to_csv(word_dir, ori_csv)
    

    if not skip_py_arword:
        add_pinyin(ori_csv, word_py_csv)
        trans_word_to_ar(word_py_csv, word_py_ar_csv)
    
    df = pd.read_csv(word_py_csv)
    if not skip_quiz:
        columns = df.columns.to_list()
        columns.append("quiz_id")
        df_list = df.values.tolist()
        fw = open(quiz_jsonl, "w", encoding="utf-8")
        for i in tqdm(range(df.shape[0])):
            word = df.iloc[i]["单词"]
            py = df.iloc[i]["拼音"]
            print (word)
            # try:
            content = generate_sent_quiz(word, py)
            if content["answer"] !=  py:
                content["answer"] = py
            idx2alp = ["A", "B", "C", "D"]
            rd.shuffle(content["options"])
            for opt_idx, opt in enumerate(content["options"]):
                if opt.strip() == content["answer"].strip():
                    content["answer"] = idx2alp[opt_idx]
            if not content["answer"] in idx2alp:
                print ("{} answer error".format(word))
                content["answer"] = "C"
                content["options"][2] = py
            for opt_idx in range(len(content["options"])):
                content["options"][opt_idx] = "{}. {}".format(idx2alp[opt_idx], content["options"][opt_idx])
            
            # multi_lingual_quiz = video_processor.translate_zh_quiz(content)
            content["ar_question"] = "ما هو بينيين ل{}؟".format(word)
            content["ar_options"] = content["options"]
            content["ar_explanation"] = ""
            content["en_question"] = "What is the pinyin of {}?".format(word)
            content["en_options"] = content["options"]
            content["en_explanation"] = ""
            content["vid"] = "{}_{}".format(i, word)
            df_list[i].append(content["vid"])
            fw.write("{}\n".format(json.dumps(content, ensure_ascii=False)))
                
            # except Exception as e:
            #     print (e)
            #     print ("{} error".format(word))
            #     import pdb;pdb.set_trace()
            #     time.sleep(5)

        df_new = pd.DataFrame(df_list, columns=columns)
        df_new.to_csv(quiz_csv, index=False)
    
    if not skip_srt:
        df_video = pd.read_csv(quiz_csv)
        columns = df_video.columns.to_list()
        columns += ["zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
        df_list = df_video.values.tolist()
        for i in tqdm(range(df_video.shape[0])):
            try:
                word = df_video.iloc[i]["单词"]
                srt_prefix = "{}_{}".format(i, word)

                srt_res = generate_subtitle([word], srt_prefix, srt_dir, 3000)
                df_list[i].append(srt_res["zh_srt"])
                df_list[i].append(srt_res["ar_srt"])
                df_list[i].append(srt_res["en_srt"])
                df_list[i].append(srt_res["pinyin_srt"])
            except Exception as e:
                print (e)
                print ("{} error".format(word))
                time.sleep(5)
        df_srt = pd.DataFrame(df_list, columns=columns)
        df_srt.to_csv(srt_csv, index=False)



    # df_srt = pd.read_csv("沙特女子Demo/初级汉语/词汇练习/words_refined_with_sent_pinyin_ar_video_srt.csv")
    # df_srt.dropna(subset=["zh_srt"], axis=0, how="any", inplace=True)
    # df_srt.to_csv("沙特女子Demo/初级汉语/词汇练习/words_refined_with_sent_pinyin_ar_video_srt_clean.csv", index=False)
    
    if not skip_tag:
        from content_tagger import update_video_info_csv_level
        # pnu1_tag_csv = "沙特女子Demo/初级汉语/词汇练习/pnu1_srt_clean_tag.csv"
        update_video_info_csv_level(srt_csv, tag_csv)
        # pnu2_tag_csv = "沙特女子Demo/初级汉语/词汇练习/pnu2_srt_clean_tag.csv"
        # update_video_info_csv_level("沙特女子Demo/初级汉语/词汇练习/pnu2_srt_clean.csv", pnu2_tag_csv)
    if not skip_vod:
        from vod_hw_util import upload_hw_withcsv
        # pnu1_vod_csv = "沙特女子Demo/初级汉语/词汇练习/pnu1_srt_clean_tag_vod.csv"
        # upload_hw_withcsv(pnu1_tag_csv, pnu1_vod_csv)
        # pnu2_vod_csv = "沙特女子Demo/初级汉语/词汇练习/pnu2_srt_clean_tag_vod.csv"
        upload_hw_withcsv(tag_csv, vod_csv)

    if not skip_create:
        from create_video import create_with_csv, update_videoinfo_recommender_withcsv
        # pnu1_create_csv = "沙特女子Demo/初级汉语/词汇练习/pnu1_srt_clean_tag_vod_create.csv"
        # create_with_csv("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/词汇练习/words_quiz.jsonl", pnu1_vod_csv, pnu1_create_csv, customize="PNU_1", series_name="PNU_1")
        # update_videoinfo_recommender_withcsv(pnu1_create_csv)

        # pnu2_create_csv = "沙特女子Demo/初级汉语/词汇练习/pnu2_srt_clean_tag_vod_create.csv"
        create_with_csv(quiz_jsonl, vod_csv, create_csv, customize=create_tag)
        # update_videoinfo_recommender_withcsv(pnu2_create_csv)


    
