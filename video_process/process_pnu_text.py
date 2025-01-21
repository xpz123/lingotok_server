import cv2
import os
import pysrt
import re
from tqdm import tqdm
import pandas as pd
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from moviepy.editor import VideoFileClip, AudioFileClip, TextClip, CompositeVideoClip, ImageClip
from moviepy.config import change_settings
change_settings({"IMAGEMAGICK_BINARY": "/opt/homebrew/Cellar/imagemagick/7.1.1-43/bin/magick"})
import urllib.request
from retry import retry
from pydub import AudioSegment
from video_processor import VideoProcessor, milliseconds_to_time_string
from translator import translate_text2ar
import shutil
from vod_huoshan_util import upload_media
from datetime import datetime
from bidi.algorithm import get_display
import arabic_reshaper
from huoshan_tts_util import generate_wav
from aigc_new import merge_audios, add_audio_to_video
import copy
from process_pnu_words import generate_subtitle
import json
import shutil

def convert_imagedir(image_dir):
    # cp jpg to png
    for root, dirs, files in os.walk(image_dir):
        for file in files:
            if file.endswith(".jpg"):
                jpg_path = os.path.join(root, file)
                png_path = jpg_path.replace(".jpg", ".png")
                shutil.copy(jpg_path, png_path)

def generate_quiz(srt_csv, output_csv, quiz_jsonl):
    video_processor = VideoProcessor()
    df = pd.read_csv(srt_csv)
    columns = df.columns.to_list()
    columns.append("quiz_id")
    df_list = df.values.tolist()
    fw = open(quiz_jsonl, "w", encoding="utf-8")
    for i in range(df.shape[0]):
        df_list[i].append(df.iloc[i]["title"])
        content = video_processor.generate_quiz_zh_tiankong_v2(df.iloc[i]["zh_srt"])
        multi_lingual_quiz = video_processor.translate_zh_quiz(content)
        content["ar_question"] = multi_lingual_quiz["ar_quiz"]["question"]
        content["ar_options"] = multi_lingual_quiz["ar_quiz"]["options"]
        content["ar_explanation"] = multi_lingual_quiz["ar_quiz"]["explanation"]
        content["en_question"] = multi_lingual_quiz["en_quiz"]["question"]
        content["en_options"] = multi_lingual_quiz["en_quiz"]["options"]
        content["en_explanation"] = multi_lingual_quiz["en_quiz"]["explanation"]
        content["vid"] = df.iloc[i]["title"]
        fw.write(json.dumps(content, ensure_ascii=False) + "\n")
    fw.close()
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(output_csv, index=False)

        


def cut_sentences(ori_file, sents_file):
    lines = open(ori_file).readlines()
    # text = " ".join(lines).replace("\n", " ")
    # print (text)
    # # 定义更复杂的正则表达式以处理引号和省略号
    # pattern = r'(?<=[。！？?])\s*'
    # sentences = re.split(pattern, text)
    
    # 进一步处理引号内的内容
    # sentences = [s.strip() for s in sentences if s.strip()]

    file_list = list()
    sent_list = list()
    res = list()
    for idx, sent in enumerate(lines):
        sent_list.append(sent.strip())
        file_list.append(ori_file.split("/")[-1].replace(".txt", "") + "_sent{}.txt".format(idx+1))
        res.append((ori_file.split("/")[-1].replace(".txt", "") + "_sent{}".format(idx+1), sent))
    tmp_dcit = {"filename": file_list, "script": sent_list}
    df = pd.DataFrame(tmp_dcit)
    df.to_csv(sents_file, index=False)
    return res

def images_to_video(audio_dur_dict, img_list, output_video, audio_name_list, fps=25):
    # image_dir = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1"
    # image_dict = {"sent1": 6.18, "sent2": 18.3, "sent3": 27.36, "sent4": 38.26}
    # image_list = ["sent1", "sent2", "sent3", "sent4"]

    # 读取第一张图片以获取尺寸
    print (img_list[0])
    first_image = cv2.imread(img_list[0])
    height, width, layers = first_image.shape
    print (height, width, layers)

    # 创建VideoWriter对象
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # 使用mp4v编码
    video_writer = cv2.VideoWriter(output_video, fourcc, fps, (width, height))


    # 遍历所有图片并写入视频
    # pre_end_time = 0

    for idx, img_path in enumerate(img_list):
        dur = audio_dur_dict[audio_name_list[idx]]
        if not os.path.exists(img_path):
            print ("{} not exists".format(img_path))
            img = cv2.imread(img_list[idx-1])
        else:
            img = cv2.imread(img_path)
        resized_img = cv2.resize(img, (width, height))
        image_repeat_num = int(fps * dur)
        for _ in range(image_repeat_num):
            video_writer.write(resized_img)

    # 释放VideoWriter对象
    cv2.destroyAllWindows()
    video_writer.release()
    print(f'视频已保存为: {output_video}')



def create_aigc_csv(ori_file_list, out_csv_file):
    columns = ["ori_file","sents_csv", "FileName", "zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
    df_list = list()
    for ori_file in ori_file_list:
        line_list = list()
        line_list.append(ori_file)
        root_dir = ori_file.replace(".txt", "")
        video_dir = os.path.join(root_dir, "video")
        sents_csv = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_sents.csv"))
        line_list.append(sents_csv)
        
        final_video_path = os.path.join(video_dir, ori_file.split("/")[-1].replace(".txt", "_sents.mp4"))
        line_list.append(final_video_path)
        zh_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_Chinese.srt"))
        line_list.append(zh_srt_path)
        en_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_English.srt"))
        line_list.append(en_srt_path)
        ar_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_Arabic.srt"))
        line_list.append(ar_srt_path)
        pinyin_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_Pinyin.srt"))
        line_list.append(pinyin_srt_path)
        df_list.append(line_list)
    df = pd.DataFrame(df_list, columns=columns)
    df.to_csv(out_csv_file, index=False)

def split_pnu_csv(ori_csv, ori_excel=None):
    if ori_excel:
        df = pd.read_excel(ori_excel)
    else:
        df = pd.read_csv(ori_csv)
    pre_sid = df.iloc[0]["对话编号"]
    all_sent_list = list()
    sent_list = list()
    image_list = list()
    for i in range(df.shape[0]):
        sid = df.iloc[i]["对话编号"]
        speak_id = df.iloc[i]["说话人"]
        script = df.iloc[i]["语句"]
        if sid != pre_sid:
            tmp_list = copy.deepcopy(sent_list)
            all_sent_list.append({"sid": pre_sid, "data": tmp_list, "images": image_list})
            sent_list = list()
            image_list = list()
            pre_sid = sid
        sent_list.append((speak_id, script))
        # AB050-1-186-1.png
        if "图片" not in df.columns:
            image_list.append("{}-{}-{}-{}.png".format(df.iloc[i]["index"], df.iloc[i]["行数"], df.iloc[i]["页数"],speak_id))
        # image_list.append("{}-{}-{}-{}.png".format(sid, df.iloc[i]["语句编号"], df.iloc[i]["页码"], speak_id))
        else:
            image_list.append(df.iloc[i]["图片"]+ ".png")
    tmp_list = copy.deepcopy(sent_list)
    all_sent_list.append({"sid": sid, "data": tmp_list, "images": image_list})
    return all_sent_list
        


if __name__ == '__main__':
    ori_file_list = []
    # df = pd.read_excel("沙特女子Demo/初级汉语/初级汉语课本3/完整-课本3-课本3-君-睡不醒的冬三月/1_课本.xls")
    # df = pd.read_excel("沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1/PNU数据标记_Easylove_v4.xls")
    # df.to_csv("沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1/pnu1_61.csv", index=False)
    # df.to_csv("沙特女子Demo/初级汉语/初级汉语课本3/ori.csv", index=False)
    voice_dict = {1: "BV002_streaming", 2: "BV001_streaming"}
    prefix = "初级口语2"
    # prefix = "pnu2"
    # root_dir = "沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1"
    root_dir = "沙特女子Demo/初级汉语/真-初级口语1+2/初级口语2"
    # all_sent_list = split_pnu_csv("沙特女子Demo/初级汉语/初级汉语课本3/{}.csv".format(prefix))
    # audio_dir = "沙特女子Demo/初级汉语/初级汉语课本3/audios/{}".format(prefix)
    # srt_dir = "沙特女子Demo/初级汉语/初级汉语课本3/srt/{}".format(prefix)
    # video_dir = "沙特女子Demo/初级汉语/初级汉语课本3/videos/{}".format(prefix)
    # image_dir = "沙特女子Demo/初级汉语/初级汉语课本3/完整-课本3-课本3-君-睡不醒的冬三月"

    ori_excel_file = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/真-初级口语1+2/初级口语2/更改后0115P1P18_v3.xlsx"
    df = pd.read_excel(ori_excel_file)
    # df.to_csv(os.path.join(root_dir, "chap1.csv"), index=False)
    # all_sent_list = split_pnu_csv(os.path.join(root_dir, "pnu1_61.csv"))
    all_sent_list = split_pnu_csv(os.path.join(root_dir, "chap1.csv"), ori_excel=ori_excel_file)
    audio_dir = os.path.join(root_dir, "audios")
    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)
    srt_dir = os.path.join(root_dir, "srt")
    if not os.path.exists(srt_dir):
        os.makedirs(srt_dir)
    video_dir = os.path.join(root_dir, "videos")
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    image_dir = os.path.join(root_dir, "images")
    assert os.path.exists(image_dir)
    # convert_imagedir(image_dir)

    skip_video = True
    skip_quiz = True
    skip_tag = True
    skip_vod = False
    skip_create = False
    srt_csv = os.path.join(root_dir, "srt.csv")
    quiz_csv = os.path.join(root_dir, "srt_quiz.csv")
    quiz_jsonl = os.path.join(root_dir, "srt_quiz.jsonl")
    tag_csv = quiz_csv.replace("_quiz", "_tag")
    vod_csv = quiz_csv.replace("_quiz", "_vod")
    create_csv = quiz_csv.replace("_quiz", "_create")

    columns = ["FileName", "title", "series_name", "customize", "zh_srt", "en_srt", "ar_srt", "pinyin_srt"]
    df_list = list()
    if not skip_video:
        for sent_list in tqdm(all_sent_list):
            try:
                item_list = list()
                tmp_list = list()
                sid = sent_list["sid"]
                data = sent_list["data"]
                image_list = [os.path.join(image_dir, img_name) for img_name in sent_list["images"]]
                audio_list = list()
                audio_name_list = list()
                text_list = []
                for idx, item in enumerate(data):
                    audio_name =  "{}_{}.wav".format(sid, idx)
                    audio_name_list.append("{}_{}".format(sid, idx))
                    audio_path = os.path.join(audio_dir, audio_name)
                    audio_list.append(audio_path)
                    if not os.path.exists(audio_path):
                        generate_wav(item[1], audio_path, voice_dict.get(item[0], "BV002_streaming"), speed=0.7)
                    text_list.append(item[1])
                merged_audio_path = os.path.join(audio_dir, "{}_merged.wav".format(sid))
                audio_dur_dict = merge_audios(audio_list, merged_audio_path)
                srt_res = generate_subtitle(text_list, sid, srt_dir, audio_dur_dict, audio_list=audio_name_list)

                video_path = os.path.join(video_dir, "{}.mp4".format(sid))
                mute_video_path = os.path.join(video_dir, "{}_mute.mp4".format(sid))
                if not os.path.exists(video_path):
                    images_to_video(audio_dur_dict, image_list, mute_video_path, audio_name_list=audio_name_list)
                    add_audio_to_video(mute_video_path, merged_audio_path, video_path)
                
                item_list += [video_path, sid, "{}-课文".format(prefix), prefix.upper(), srt_res["zh_srt"], srt_res["en_srt"], srt_res["ar_srt"], srt_res["pinyin_srt"]]
                df_list.append(item_list)
            except Exception as e:
                import pdb;pdb.set_trace()
                print (e)
                print ("error in {}".format(sent_list["sid"]))
        df = pd.DataFrame(df_list, columns=columns)
        df.to_csv(srt_csv, index=False)

    # srt_csv = "沙特女子Demo/初级汉语/初级汉语课本3/{}_srt.csv".format(prefix)
    # df.to_csv("沙特女子Demo/初级汉语/初级汉语课本3/{}_srt.csv".format(prefix), index=False)

    # quiz_csv = "沙特女子Demo/初级汉语/初级汉语课本3/{}_quiz.csv".format(prefix)
    # quiz_jsonl = "沙特女子Demo/初级汉语/初级汉语课本3/{}_quiz.jsonl".format(prefix)
    if not skip_quiz:
        generate_quiz(srt_csv, quiz_csv, quiz_jsonl)

    if not skip_tag:
        from content_tagger import update_video_info_csv_level
        
        update_video_info_csv_level(quiz_csv, tag_csv)
    
    if not skip_vod:
        from vod_hw_util import upload_hw_withcsv
        
        upload_hw_withcsv(tag_csv, vod_csv)
    
    if not skip_create:
        from create_video import create_with_csv, update_videoinfo_recommender_withcsv

        create_with_csv(quiz_jsonl, vod_csv, create_csv, customize="PNU_2_1")
    # update_videoinfo_recommender_withcsv(create_csv)

