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

def generate_subtitle(zh_text_list, srt_prefix, root_dir, audio_dur_dict, audio_list=None):
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
        if audio_list is not None:
            dur_ms = int(audio_dur_dict[audio_list[i]] * 1000)
        else:
            dur_ms = int(audio_dur_dict["{}_{}".format(zh_text_list[0], (i+1))] * 1000)
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
def generate_sent_quiz(word):
    json_str = json.dumps({"sentence": "我跟朋友分别时会说再见。", "question": "我跟朋友分别时会说____。", "options": ["再见", "看见", "再一次", "见到"], "answer": "再见", "explanation": "“再见”是人们在分别时常使用的礼貌用语，而“再说、看见、再一次”并不是适合语境的表达，不符合日常用语习惯，所以应选“再见”。"})
    prompt = "请根据下面给出的中文单词进行造句，同时将其变成一个四选一的题目。要注意选项中的单词除了正确选项外，其余要是一个常用的中文词汇，但非常不适合填在句子中。同时要说明选择正确选项的理由。结果用Json格式进行返回，json格式如下{}。中文单词：{}".format(json_str, word)
    resp = call_doubao_pro_32k(prompt)
    content_str = resp.replace("```json", "").replace("```", "")
    print (content_str)
    content = json.loads(content_str)
    return content
    
def modify_videos(emoji_path, word, pinyin, ar_word, merged_audio, audio_dur_dict, out_video_path):
    audio_dur = sum([audio_dur_dict[key] for key in audio_dur_dict.keys()])
    try:
        if emoji_path.endswith(".gif") or emoji_path.endswith(".GIF"):
            emoji_path = emoji_path.replace(".gif", ".mp4").replace(".GIF", ".mp4")
            # cmd = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg -loglevel error -y -i  {} -b:v 1M -vf scale=540:-1 {}".format(emoji_path, os.path.join(root_dir, "tmp.mp4"))
            # os.system(cmd)
            video_ori = VideoFileClip(emoji_path)
            if audio_dur > video_ori.duration:
                n_loop = int(audio_dur / video_ori.duration) + 1
                print (n_loop)
            video_ori = video_ori.loop(n=n_loop)
            # os.remove(os.path.join(root_dir, "tmp.mp4"))
            # video_ori = VideoFileClip(os.path.join(root_dir, "tmp_loop.mp4"))
            # os.remove(os.path.join(root_dir, "tmp_loop.mp4"))   
        else:
            fps = 10
            image_num = int(fps * audio_dur)
            images = [emoji_path for i in range(image_num)]
            video_ori = ImageSequenceClip(images, fps=fps)
        # video_no_audio=video_ori.set_audio(None)

        video = video_ori.subclip(0, audio_dur)
        # video_audio = video.audio.volumex(0.05)
        max_val = max(video.size[0], video.size[1])
        if max_val < 540:
            resize_ratio = max(1, 540.0 / float(max_val))
            new_w = resize_ratio * video.w
            if new_w % 2 == 1:
                new_w += 1
            new_h = resize_ratio * video.h
            if new_h % 2 == 1:
                new_h += 1
            video = video.resize(newsize=(new_w, new_h))
        
            
        txt_clip_chinese = TextClip("{}\n{}".format(pinyin, word), fontsize=35, color='white', font='Songti-SC-Black')
        txt_clip_chinese = txt_clip_chinese.set_position("center", relative=True).set_duration(video.duration)

        # print (info["arabic"])
        # print (get_display(info["arabic"]))
        # r2l_arbic = arabic_reshaper.reshape(get_display(info["arabic"]))
        r2l_arbic = get_display(arabic_reshaper.reshape(ar_word))
        # print (r2l_arbic)
        txt_clip_arbic = TextClip(r2l_arbic, fontsize=35, color='white', font='Almarai')
        txt_clip_arbic = txt_clip_arbic.set_position("center", relative=True).set_duration(video.duration)

        txt_w = max(txt_clip_arbic.w, txt_clip_chinese.w)
        chinese_bg_color = ColorClip(size=(txt_w, txt_clip_chinese.h), color=(0, 0, 0), duration=video.duration)
        chinese_bg_color = chinese_bg_color.set_opacity(0.7)
        chinese_text_with_bg = CompositeVideoClip([chinese_bg_color, txt_clip_chinese])
        chinese_text_with_bg = chinese_text_with_bg.set_position(("center", 0.2), relative=True).set_duration(video.duration)
        arbic_text_with_bg = ColorClip(size=(txt_w, txt_clip_arbic.h), color=(0, 0, 0), duration=video.duration)
        arbic_text_with_bg = arbic_text_with_bg.set_opacity(0.7)
        arbic_text_with_bg = CompositeVideoClip([arbic_text_with_bg, txt_clip_arbic])
        
        arbic_h = float(video.size[1] * 0.2 + chinese_text_with_bg.h) / float(video.size[1])
        arbic_text_with_bg = arbic_text_with_bg.set_position(("center", arbic_h), relative=True).set_duration(video.duration)
        video_with_text = CompositeVideoClip([video, chinese_text_with_bg, arbic_text_with_bg])
        audio_clip = AudioFileClip(merged_audio)

        # final_audio = CompositeAudioClip([audio_clip, video_audio])

        video_final = video_with_text.set_audio(audio_clip)
        # video_final.write_videofile(out_video_path, codec='libx264', audio_codec="aac")

        
        # video_final = CompositeVideoClip([video_final, pre_text_clip_with_bg, word_clip_withbg, post_text_clip_withbg])
        video_final.write_videofile(out_video_path, codec='libx264', audio_codec="aac")

        return 1
    except Exception as e:
        print (e)
        print ("{} error".format(word))
        return -1
        
def upload_video(video_srt_file):
    
    df = pd.read_csv(video_srt_file)
    df.dropna(subset=["zh_srt"], axis=0, how="any", inplace=True)

def gif_to_mp4(gif_path):
    cmd = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg -loglevel error -y -i  \"{}\" -vf \"scale=trunc(iw/2)*2:trunc(ih/2)*2\" -c:v libx264 -pix_fmt yuv420p -movflags +faststart -crf 23 \"{}\"".format(gif_path, gif_path.replace(".gif", ".mp4").replace(".GIF", ".mp4"))
    os.system(cmd)

def convert_emojidir_to_csv(emoji_dir, out_csv):
    df_list = list()
    leagal_suffix = [".jpg", ".jpeg", ".png", ".gif", ".JPG", ".JPEG", ".PNG", ".GIF"]
    for item in os.listdir(emoji_dir):
        is_legual = False
        for suffix in leagal_suffix:
            if item.endswith(suffix):
                is_legual = True
                break
        if not is_legual:
            continue
        word = item.split(".")[0]
        if item.endswith(".gif") or item.endswith(".GIF"):
            gif_to_mp4(os.path.join(emoji_dir, item))
            # item = item.replace(".gif", ".mp4").replace(".GIF", ".mp4")
        df_list.append([word, os.path.join(emoji_dir, item)])
    df = pd.DataFrame(df_list, columns=["单词", "emoji"])
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
    root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0203"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0212"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0219"
    emoji_dir = os.path.join(root_dir, "学中文表情包")
    # create_tag = "KAU777"
    create_tag = "PNU888"

    audio_dir = os.path.join(root_dir, "audios")
    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)
    video_dir = os.path.join(root_dir, "videos")
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    
    emoji_csv = os.path.join(root_dir, "emoji.csv")
    quiz_jsonl = os.path.join(root_dir, "words_quiz.jsonl")
    quiz_csv = os.path.join(root_dir, "quiz.csv")
    word_py_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin.csv")
    word_py_ar_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin_ar.csv")
    video_csv = os.path.join(root_dir, "video.csv")
    srt_csv = os.path.join(root_dir, "srt.csv")
    tag_csv = os.path.join(root_dir, "tag.csv")
    vod_csv = os.path.join(root_dir, "vod.csv")
    create_csv = os.path.join(root_dir, "create.csv")

    skip_outdate = True
    skip_quiz = True
    skip_py_arword = True
    skip_video = True
    skip_srt = True
    skip_tag = True
    skip_vod = False
    skip_create = False

    if not skip_outdate:
        # prev_csv = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0126/create.csv"
        # prev_csv = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0203/create.csv"
        prev_csv = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/KAU-lecture/0212/create.csv"
        # outdate_with_csv(prev_csv, "0126")
        # outdate_with_csv(prev_csv, "0203")
        outdate_with_csv(prev_csv, "0212")


    convert_emojidir_to_csv(emoji_dir, emoji_csv)
    df = pd.read_csv(emoji_csv)

    if not skip_quiz:
        columns = df.columns.to_list()
        columns.append("例句")
        columns.append("问题")
        columns.append("quiz_id")
        df_list = df.values.tolist()
        fw = open(quiz_jsonl, "w", encoding="utf-8")
        for i in tqdm(range(df.shape[0])):
            word = df.iloc[i]["单词"]
            print (word)
            try:
                content = generate_sent_quiz(word)
                idx2alp = ["A", "B", "C", "D"]
                rd.shuffle(content["options"])
                for opt_idx, opt in enumerate(content["options"]):
                    if opt == content["answer"]:
                        content["answer"] = idx2alp[opt_idx]
                        break
                if not content["answer"] in idx2alp:
                    print ("{} answer error".format(word))
                    continue
                for opt_idx in range(len(content["options"])):
                    content["options"][opt_idx] = "{}. {}".format(idx2alp[opt_idx], content["options"][opt_idx])
                
                multi_lingual_quiz = video_processor.translate_zh_quiz(content)
                content["ar_question"] = multi_lingual_quiz["ar_quiz"]["question"]
                content["ar_options"] = multi_lingual_quiz["ar_quiz"]["options"]
                content["ar_explanation"] = multi_lingual_quiz["ar_quiz"]["explanation"]
                content["en_question"] = multi_lingual_quiz["en_quiz"]["question"]
                content["en_options"] = multi_lingual_quiz["en_quiz"]["options"]
                content["en_explanation"] = multi_lingual_quiz["en_quiz"]["explanation"]
                content["vid"] = "{}_{}".format(i, word)
                df_list[i].append(content["sentence"])
                df_list[i].append(json.dumps(content, ensure_ascii=False))
                df_list[i].append(content["vid"])
                fw.write("{}\n".format(json.dumps(content, ensure_ascii=False)))
                
            except Exception as e:
                print (e)
                print ("{} error".format(word))
                time.sleep(5)

        df_new = pd.DataFrame(df_list, columns=columns)
        df_new.to_csv(quiz_csv, index=False)
    
    if not skip_py_arword:
        add_pinyin(quiz_csv, word_py_csv)
        # trans_word_to_ar(word_py_csv, word_py_ar_csv)

    if not skip_video:
        # fj_video_dir = "/Users/tal/work/lingtok_server/video_process/hw/videos/风景视频"
        # video_path_list = [os.path.join(fj_video_dir, item) for item in os.listdir(fj_video_dir)]
        # rd.shuffle(video_path_list)

        df_new = pd.read_csv(word_py_ar_csv)
        # df_new = pd.read_csv("沙特女子Demo/初级汉语/词汇练习/words_refined_with_sent_pinyin_ar_video.csv")
        columns = df_new.columns.to_list()
        columns.append("FileName")
        columns.append("with_sent")
        df_list = df_new.values.tolist()

        for i in tqdm(range(df_new.shape[0])):
            try:
                emoji_path = df_new.iloc[i]["emoji"]
                word = df_new.iloc[i]["单词"]
                py = df_new.iloc[i]["拼音"]
                ar_word = df_new.iloc[i]["阿语翻译"]
                content = json.loads(df_new.iloc[i]["问题"])
                audio_list = [os.path.join(audio_dir, "{}_1.wav".format(word)), os.path.join(audio_dir, "{}_2.wav".format(word)).format(word), os.path.join(audio_dir, "{}_1.wav".format(word))]
                if word == "666":
                    word = "六六六"
                if not os.path.exists(audio_list[1]):
                    generate_wav(word, audio_list[1], voice_type="BV001_streaming", speed=0.3)
                if not os.path.exists(audio_list[0]):
                    generate_wav(word, audio_list[0], voice_type="BV002_streaming", speed=0.3)
                # if not os.path.exists(audio_list[2]):
                #     generate_wav(content["sentence"], audio_list[2], voice_type="BV001_streaming", speed=0.7)
                # if not os.path.exists(audio_list[3]):
                #     generate_wav(content["sentence"], audio_list[3], voice_type="BV002_streaming", speed=0.8)
                merged_wav = os.path.join(audio_dir, "{}_merged.wav".format(word))
                audio_dur_dict = merge_audios(audio_list, merged_wav, sil_dur=1000)
                # rd.shuffle(video_path_list)
                out_video = os.path.join(video_dir, "{}_{}.mp4".format(i, word))
                mark = modify_videos(emoji_path, word, py, ar_word, merged_wav, audio_dur_dict, out_video)
                if len(df_list[i]) == 27:
                    df_list[i][-2] = out_video
                    df_list[i][-1] = mark
                else:
                    df_list[i].append(out_video)
                    df_list[i].append(mark)
                
            except Exception as e:
                print (e)
                print ("{} error".format(word))
        
        
        df_video = pd.DataFrame(df_list, columns=columns)
        df_video.to_csv(video_csv, index=False)

    if not skip_srt:
        df_video = pd.read_csv(video_csv)
        columns = df_video.columns.to_list()
        columns += ["zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
        srt_dir = "沙特女子Demo/初级汉语/词汇练习/srt_dir"
        df_list = df_video.values.tolist()
        for i in tqdm(range(df_video.shape[0])):
            try:
                if df_video.iloc[i]["with_sent"] == -1:
                    continue
                word = df_video.iloc[i]["单词"]
                sent = df_video.iloc[i]["例句"]
                srt_prefix = "{}_{}".format(i, word)
                audio_list = [os.path.join(audio_dir, "{}_1.wav".format(word)), os.path.join(audio_dir, "{}_2.wav".format(word)).format(word), os.path.join(audio_dir, "{}_1.wav".format(word))]
                merged_wav = "testdir/test.wav"
                audio_name_list = ["{}_1".format(word), "{}_2".format(word), "{}_1".format(word)]
                audio_dur_dict = merge_audios(audio_list, merged_wav)
                srt_res = generate_subtitle([word, word, word], srt_prefix, srt_dir, audio_dur_dict, audio_list=audio_name_list)
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


    
