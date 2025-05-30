import pandas as pd
import os
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from translator import translate_text2ar
from pypinyin import pinyin

from moviepy.editor import VideoFileClip, AudioFileClip, TextClip, CompositeVideoClip, ColorClip, concatenate_videoclips, CompositeAudioClip
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
from translator import Translator

translator = Translator()

def refine_ar_word(ori_csv_file):
    translator = Translator()
    df = pd.read_csv(ori_csv_file)
    for i in tqdm(range(df.shape[0])):
        zh_word = df.iloc[i]["单词"]
        ar_word = translator.translate_zhword(zh_word)["ar"]
        if ar_word != "":
            df.iloc[i]["阿语翻译"] = ar_word
    df.to_csv(ori_csv_file, index=False)

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

    # en_text_list = translate_text2ar(zh_text_list, "en")
    # assert len(en_text_list) == len(zh_text_list)

    fw_zh = open(zh_srt, "w")
    fw_ar = open(ar_srt, "w")
    # fw_en = open(en_srt, "w")
    
    start_time = 0
    for i in range(len(zh_text_list)):
        if audio_list is not None:
            dur_ms = int(audio_dur_dict[audio_list[i]] * 1000)
        else:
            dur_ms = int(audio_dur_dict["{}_{}".format(zh_text_list[0], i+1)] * 1000)
        start_time_str = milliseconds_to_time_string(start_time)
        end_time_str = milliseconds_to_time_string(start_time + dur_ms)
        start_time = start_time + dur_ms
        zh_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{zh_text_list[i]}\n\n"
        fw_zh.write(zh_srt_content)

        ar_word = translator.translate_zhword(zh_text_list[i])["ar"]
        if ar_word == "":
            ar_word = ar_text_list[i]["Translation"]

        ar_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{ar_word}\n\n"
        fw_ar.write(ar_srt_content)

        # en_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{en_text_list[i]['Translation']}\n\n"
        # fw_en.write(en_srt_content)
    fw_zh.close()
    fw_ar.close()
    # fw_en.close()
    zh_word = zh_text_list[0]
    en_word = translator.translate_zhword(zh_word)["en"]
    if en_word != "":
        translator.translate_word_sent_zhsrt2ensrt_with_keyword(zh_srt, en_srt, keyword=(zh_word, en_word))
    else:
        translator.translate_zhsrt2ensrt_with_context(zh_srt, en_srt)
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
    
def modify_videos(video_path, word, pinyin, ar_word, sentence, merged_audio, audio_dur_dict, out_video_path):
    audio_dur = sum([audio_dur_dict[key] for key in audio_dur_dict.keys()])
    try:
        video_ori = VideoFileClip(video_path)
        # video_no_audio=video_ori.set_audio(None)

        while audio_dur > video_ori.duration:
            clips = [video_ori, video_ori]
            video_ori = concatenate_videoclips(clips)
        video = video_ori.subclip(0, audio_dur)
        video_audio = video.audio.volumex(0.05)
        # max_val = max(video.size[0], video.size[1])
        # if max_val > 720:
        # resize_ratio = min(1,  / max_val)
        # new_w = resize_ratio * video.w
        # if new_w % 2 == 1:
        #     new_w += 1
        # new_h = resize_ratio * video.h
        # if new_h % 2 == 1:
        #     new_h += 1
        # video = video.resize(newsize=(new_w, new_h))
            
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
        # video_with_text = CompositeVideoClip([video, chinese_text_with_bg, arbic_text_with_bg])
        video_with_text = CompositeVideoClip([video, chinese_text_with_bg])
        audio_clip = AudioFileClip(merged_audio)

        final_audio = CompositeAudioClip([audio_clip, video_audio])

        video_final = video_with_text.set_audio(final_audio)
        # video_final.write_videofile(out_video_path, codec='libx264', audio_codec="aac")

        
        if sentence.find(word) == -1:
            print ("{} not in sentence".format(word))
            video_final.write_videofile(out_video_path, codec='libx264', audio_codec="aac")
            return 0
        sent_clip = TextClip(sentence, fontsize=25, color='white', font='Songti-SC-Black')
        if sent_clip.w > video_final.w * 0.9:
            print ("sentence too long, skip!")
            video_final.write_videofile(out_video_path, codec='libx264', audio_codec="aac")
            return 0
        start_pos = (1.0 - sent_clip.w / video_final.w) / 2.0
        pre_sent = sentence.split(word)[0]
        if pre_sent == "":
            pre_sent = " "
        pre_text_clip = TextClip(pre_sent, fontsize=25, color='white', font='Songti-SC-Black').set_duration(video.duration)
        # pre_text_clip = pre_text_clip.set_position((start_pos, 0.5), relative=True).set_duration(video.duration)
        pre_text_bg = ColorClip(size=pre_text_clip.size, color=(0, 0, 0), duration=video.duration)
        pre_text_clip_with_bg = CompositeVideoClip([pre_text_bg, pre_text_clip])
        pre_text_clip_with_bg = pre_text_clip_with_bg.set_position((start_pos, 0.5), relative=True).set_duration(video.duration)

        word_clip = TextClip(word, fontsize=25, color='red', font='Songti-SC-Black').set_duration(video.duration)
        # word_clip = word_clip.set_position((start_pos + pre_text_clip.w / video_final.w, 0.5), relative=True)
        word_bg = ColorClip(size=word_clip.size, color=(0, 0, 0), duration=video.duration)
        word_clip_withbg = CompositeVideoClip([word_bg, word_clip])
        word_clip_withbg = word_clip_withbg.set_position((start_pos + pre_text_clip_with_bg.w / video_final.w, 0.5), relative=True).set_duration(video.duration)

        post_sent = (word).join(sentence.split(word)[1:])
        post_text_clip = TextClip(post_sent, fontsize=25, color='white', font='Songti-SC-Black').set_duration(video.duration)
        # post_text_clip = post_text_clip.set_position((start_pos + pre_text_clip.w / video_final.w + word_clip.w / video_final.w, 0.5), relative=True)
        post_bg = ColorClip(size=post_text_clip.size, color=(0, 0, 0), duration=video.duration)
        post_text_clip_withbg = CompositeVideoClip([post_bg, post_text_clip])
        post_text_clip_withbg = post_text_clip_withbg.set_position((start_pos + pre_text_clip_with_bg.w / video_final.w + word_clip.w / video_final.w, 0.5), relative=True).set_duration(video.duration)

        # sent_bg_color = ColorClip(size=(pre_text_clip.w + word_clip.w + post_text_clip.w, pre_text_clip.h), color=(0, 0, 0), duration=video.duration)
        # sent_bg_color = sent_bg_color.set_opacity(0.7)
        # sent_with_bg = CompositeVideoClip([sent_bg_color, pre_text_clip, word_clip, post_text_clip])
        # sent_with_bg = sent_with_bg.set_position((start_pos, 0.5), relative=True).set_duration(video.duration)
        video_final = CompositeVideoClip([video_final, pre_text_clip_with_bg, word_clip_withbg, post_text_clip_withbg])
        video_final.write_videofile(out_video_path, codec='libx264', audio_codec="aac")

        return 1
    except Exception as e:
        print (e)
        print ("{} error".format(word))
        return -1
        
def upload_video(video_srt_file):
    
    df = pd.read_csv(video_srt_file)
    df.dropna(subset=["zh_srt"], axis=0, how="any", inplace=True)
    



if __name__ == "__main__":
    # df = pd.read_csv("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/词汇练习/words_refined.csv")
    # df = pd.read_excel("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1/words_chap1.xls")
    # df = pd.read_excel("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/真-初级口语1+2/初级口语1/chap11/words_chap11.xls")
    
    video_processor = VideoProcessor()

    root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/词汇练习"
    
    create_tag = "风景单词卡"

    audio_dir = os.path.join(root_dir, "audios")
    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)
    video_dir = os.path.join(root_dir, "videos")
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    quiz_jsonl = os.path.join(root_dir, "words_quiz.jsonl")
    quiz_csv = os.path.join(root_dir, "quiz.csv")
    word_py_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin.csv")
    word_py_ar_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin_ar.csv")
    video_csv = os.path.join(root_dir, "video.csv")
    srt_csv = os.path.join(root_dir, "words_refined_with_sent_pinyin_ar_video_srt_clean.csv")
    tag_csv = os.path.join(root_dir, "tag.csv")
    vod_csv = os.path.join(root_dir, "vod.csv")
    create_csv = os.path.join(root_dir, "create.csv")

    skip_quiz = True
    skip_add_pinyin = True
    skip_trans_word_to_ar = True
    skip_video = True
    skip_srt = True
    skip_tag = True
    skip_vod = True
    skip_create = False
    

    if not skip_quiz:
        df = pd.read_excel(os.path.join(root_dir, "words_chap.xls"))
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

    if not skip_add_pinyin:
        add_pinyin(quiz_csv, word_py_csv)
    
    # if not skip_trans_word_to_ar:
    #     trans_word_to_ar(word_py_csv, word_py_ar_csv)

    if not skip_video:
        fj_video_dir = "/Users/tal/work/lingtok_server/video_process/hw/videos/风景视频"
        video_path_list = [os.path.join(fj_video_dir, item) for item in os.listdir(fj_video_dir)]
        rd.shuffle(video_path_list)

        # add_pinyin(quiz_csv, word_py_csv)
        # trans_word_to_ar(word_py_csv, word_py_ar_csv)

        df_new = pd.read_csv(word_py_ar_csv)
        # df_new = pd.read_csv("沙特女子Demo/初级汉语/词汇练习/words_refined_with_sent_pinyin_ar_video.csv")
        columns = df_new.columns.to_list()
        columns.append("FileName")
        columns.append("with_sent")
        df_list = df_new.values.tolist()
        

        for i in tqdm(range(df_new.shape[0])):
            try:
                word = df_new.iloc[i]["单词"]
                py = df_new.iloc[i]["拼音"]
                ar_word = df_new.iloc[i]["阿语翻译"]
                content = json.loads(df_new.iloc[i]["问题"])
                audio_list = [os.path.join(audio_dir, "{}_1.wav".format(word)), os.path.join(audio_dir, "{}_2.wav".format(word)), os.path.join(audio_dir, "{}_3.wav".format(word)), os.path.join(audio_dir, "{}_4.wav".format(word)).format(word)]
                if not os.path.exists(audio_list[0]):
                    generate_wav(word, audio_list[0], voice_type="BV001_streaming", speed=0.7)
                if not os.path.exists(audio_list[1]):
                    generate_wav(word, audio_list[1], voice_type="BV002_streaming", speed=0.8)
                if not os.path.exists(audio_list[2]):
                    generate_wav(content["sentence"], audio_list[2], voice_type="BV001_streaming", speed=0.7)
                if not os.path.exists(audio_list[3]):
                    generate_wav(content["sentence"], audio_list[3], voice_type="BV002_streaming", speed=0.8)
                merged_wav = os.path.join(audio_dir, "{}_merged.wav".format(word))
                audio_dur_dict = merge_audios(audio_list, merged_wav)
                rd.shuffle(video_path_list)
                out_video = os.path.join(video_dir, "{}_{}.mp4".format(i, word))
                mark = modify_videos(video_path_list[0], word, py, ar_word, content["sentence"], merged_wav, audio_dur_dict, out_video)
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
    
    import pdb;pdb.set_trace()

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
                audio_list = [os.path.join(audio_dir, "{}_1.wav".format(word)), os.path.join(audio_dir, "{}_2.wav".format(word)), os.path.join(audio_dir, "{}_3.wav".format(word)), os.path.join(audio_dir, "{}_4.wav".format(word)).format(word)]
                merged_wav = "testdir/test.wav"
                audio_dur_dict = merge_audios(audio_list, merged_wav)
                srt_res = generate_subtitle([word, word, sent, sent], srt_prefix, srt_dir, audio_dur_dict)
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
        create_with_csv(quiz_jsonl, vod_csv, create_csv, customize=create_tag, series_name="初级口语风景单词卡")

        import os
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from recommender.video_updater import VideoUpdater
        video_updater = VideoUpdater()
        video_updater.update_series_tag_once("初级口语风景单词卡", level="初学", tag_list=["科学教育"])
        # update_videoinfo_recommender_withcsv(pnu2_create_csv)


    
