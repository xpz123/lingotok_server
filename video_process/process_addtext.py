from video_processor import VideoProcessor, merge_audios, milliseconds_to_time_string
import os
import pandas as pd
from tqdm import tqdm
import json
from huoshan_tts_util import generate_wav
from pydub import AudioSegment
from translator import translate_text2ar, Translator
from moviepy.editor import VideoFileClip
from content_tagger import update_video_info_csv_level
from vod_hw_util import upload_hw_withcsv
from create_video import create_with_csv
import random as rd

def generate_subtitle(word, srt_prefix, srt_dir, start_time, end_time):
    video_processor = VideoProcessor()
    translator = Translator()
    res = dict()
    zh_srt = os.path.join(srt_dir, "{}_Chinese.srt".format(srt_prefix))
    ar_srt = os.path.join(srt_dir, "{}_Arabic.srt".format(srt_prefix))
    en_srt = os.path.join(srt_dir, "{}_English.srt".format(srt_prefix))
    pinyin_srt = os.path.join(srt_dir, "{}_Pinyin.srt".format(srt_prefix))

    res["zh_srt"] = zh_srt
    res["ar_srt"] = ar_srt
    res["en_srt"] = en_srt
    res["pinyin_srt"] = pinyin_srt
    
    ar_word = translator.translate_zhword(word)["ar"]
    en_word = translator.translate_zhword(word)["en"]

    fw_zh = open(zh_srt, "w")
    fw_ar = open(ar_srt, "w")
    fw_en = open(en_srt, "w")
    
    start_time_str = milliseconds_to_time_string(start_time)
    end_time_str = milliseconds_to_time_string(end_time)
    zh_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{word}\n\n"
    fw_zh.write(zh_srt_content)

    ar_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{ar_word}\n\n"
    fw_ar.write(ar_srt_content)

    en_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{en_word}\n\n"
    fw_en.write(en_srt_content)
    fw_zh.close()
    fw_ar.close()
    fw_en.close()

    video_processor.convert_zhsrt_to_pinyinsrt(zh_srt, pinyin_srt)
    return res

def mov2mp4(movdir):
    for root, dirs, files in os.walk(movdir):
        for f in tqdm(files):
            if f.endswith(".mov"):
                mp4_path = f.replace(".mov", ".mp4")
                os.system("/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg -y -loglevel error -i '{}' '{}'".format(os.path.join(root, f), os.path.join(root, mp4_path)))

def extract_word_from_llm_res(res):
    candidates = []
    for item in res:
        if item["timestamp"] < 2.0:
            continue
        candidates.append((item["timestamp"], item["word"]))
    
    rd.shuffle(candidates)
    start_time = candidates[0][0]
    word = candidates[0][1]
    return start_time, word

if __name__ == "__main__":
    video_processor = VideoProcessor()
    root_dir = "/Users/tal/work/lingtok_server/video_process/自制视频/视频加文字/tiktok trending 0515"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/自制视频/视频加文字/小红书"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/自制视频/视频加文字/抖音trending视频/视频1"
    # root_dir = "/Users/tal/work/lingtok_server/video_process/自制视频/视频加文字/抖音trending视频/视频2"
    video_dir = os.path.join(root_dir, "ori_videos")
    frame_dir = os.path.join(root_dir, "frames")
    if not os.path.exists(frame_dir):
        os.makedirs(frame_dir)
    audio_dir = os.path.join(root_dir, "audios")
    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)
    out_dir = os.path.join(root_dir, "outputs")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    words_dir = os.path.join(root_dir, "words")
    if not os.path.exists(words_dir):
        os.makedirs(words_dir)
        
    srt_dir = os.path.join(root_dir, "srt_dir")
    if not os.path.exists(srt_dir):
        os.makedirs(srt_dir)
    
    series_name = "小红书-0519-加字"
    quiz_metainfo_file = os.path.join(root_dir, "addtext_quiz_metainfo.jsonl")
    video_csv = os.path.join(root_dir, "addtext.csv")
    srt_csv = os.path.join(root_dir, "addtext_srt.csv")
    quiz_csv = os.path.join(root_dir, "addtext_quiz.csv")
    tag_csv = os.path.join(root_dir, "addtext_tag.csv")
    vod_csv_file = os.path.join(root_dir, "addtext_vod.csv")
    out_csv_file = os.path.join(root_dir, "addtext_out.csv")
    skip_addtext = False
    skip_srt =True
    skip_quiz = True
    skip_tag_video = True
    skip_vod = True
    skip_create = False
    skip_series_name = False

    if not skip_addtext:
        columns = ["FileName", "title", "word", "start_time", "duration"]
        df_list = list()
        for video_path in tqdm(os.listdir(video_dir)):
            if not video_path.endswith(".mp4"):
                continue
            try:
                prefix = video_path.split("/")[-1].split(".")[0]
                word_path = os.path.join(words_dir, "{}.json".format(prefix))
                if os.path.exists(word_path):
                    res = json.loads(open(word_path, "r", encoding="utf-8").readline())
                else:
                    res = video_processor.extract_frames_from_video(os.path.join(video_dir, video_path), frame_dir, extract_word=True, frame_interval=30)
                    with open(word_path, "w", encoding="utf-8") as f:
                        f.write(json.dumps(res, ensure_ascii=False))
                start_time, word = extract_word_from_llm_res(res)
                audio_path = os.path.join(audio_dir, "{}_{}.wav".format(prefix,word))
                if not os.path.exists(audio_path):
                    generate_wav(word, audio_path)
                repeat_num = 5
                audio_list = [os.path.join(audio_dir, "{}_{}.wav".format(prefix,word))] * repeat_num 
                
                audio_dur_dict = merge_audios(audio_list, os.path.join(audio_dir, "{}_merged.wav".format(word)), sil_dur=500)
                audio_dur = 0
                for key in audio_dur_dict.keys():
                    audio_dur += audio_dur_dict[key]
                
                audio_dur = audio_dur * repeat_num
                
                if os.path.exists(os.path.join(out_dir, "{}_modified.mp4".format(prefix))):
                    video_clip = VideoFileClip(os.path.join(out_dir, "{}_modified.mp4".format(prefix)))
                    df_list.append([os.path.join(out_dir, "{}_modified.mp4".format(prefix)), "{}_{}".format(prefix, word), word, start_time, video_clip.duration])
                else:
                    video_clip = VideoFileClip(os.path.join(video_dir, video_path))
                    video_clip = video_processor.add_audio_to_videoclip(video_clip, os.path.join(audio_dir, "{}_merged.wav".format(word)), start_time, audio_dur)
                    video_clip = video_processor.add_zhword_to_videoclip(video_clip, word, start_time, audio_dur)
                    video_clip = video_processor.add_process_bar_to_videoclip(video_clip, start_time, audio_dur)
                    video_clip.write_videofile(os.path.join(out_dir, "{}_modified.mp4".format(prefix)), codec="libx264", audio_codec="aac")
                    df_list.append([os.path.join(out_dir, "{}_modified.mp4".format(prefix)), "{}_{}".format(prefix, word), word, start_time, video_clip.duration])
                
            except Exception as e:
                print(e)
        df = pd.DataFrame(df_list, columns=columns)
        df.to_csv(video_csv, index=False)

    if not skip_srt:
        df = pd.read_csv(video_csv)
        columns = df.columns.to_list()
        columns += ["zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
        df_list = df.values.tolist()
        for i in tqdm(range(df.shape[0])):
            video_path = df.iloc[i]["FileName"]
            word = df.iloc[i]["word"]
            start_time = 0
            duration = df.iloc[i]["duration"]
            res = generate_subtitle(word, video_path.split("/")[-1].split(".")[0], srt_dir, start_time, int(1000 * duration))
            df_list[i].append(res["zh_srt"])
            df_list[i].append(res["ar_srt"])
            df_list[i].append(res["en_srt"])
            df_list[i].append(res["pinyin_srt"])
        df_srt = pd.DataFrame(df_list, columns=columns)
        df_srt.to_csv(os.path.join(srt_dir, "addtext_srt.csv"), index=False)
    
    if not skip_quiz:

        df = pd.read_csv(srt_csv)
        columns = df.columns.to_list()
        columns.append("quiz_id")

        df_list = df.values.tolist()
        fw = open(quiz_metainfo_file, "w", encoding="utf-8")
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from recommender.quiz_generator import CharPinyinQuizGeneratingWorker, CharFillingQuizGeneratingWorker, QuizGeneratingCtx


        quiz_ctx = QuizGeneratingCtx()
        quiz_worker_config = {"hsk_zh_en_ar_path": "hsk_dictionary/HSK_zh_en_ar.csv", "hsk_char_path": "hsk_dictionary/HSK_char.csv"}
        quiz_worker = CharFillingQuizGeneratingWorker(quiz_worker_config)
        pinyin_quiz_worker = CharPinyinQuizGeneratingWorker(quiz_worker_config)
        
        for i in tqdm(range(df.shape[0])):
            word = df.iloc[i]["word"]
            quiz_ctx.extracted_word = word
            if len(word) == 1:
                quiz_res = pinyin_quiz_worker.action(quiz_ctx)
                content = pinyin_quiz_worker.to_dict(quiz_res)
            else:
                quiz_res = quiz_worker.action(quiz_ctx)
                content = quiz_worker.to_dict(quiz_res)
            content["vid"] = "{}_{}".format(i, df.iloc[i]["FileName"])
            df_list[i].append(content["vid"])
            fw.write("{}\n".format(json.dumps(content, ensure_ascii=False)))
        fw.close()
        df_list = pd.DataFrame(df_list, columns=columns)
        df_list.to_csv(os.path.join(root_dir, "addtext_quiz.csv"), index=False)
    
    if not skip_tag_video:
        update_video_info_csv_level(quiz_csv, tag_csv)
    
    if not skip_vod:
        if os.path.exists(vod_csv_file):
            upload_hw_withcsv(vod_csv_file, vod_csv_file)
        else:
            upload_hw_withcsv(tag_csv, vod_csv_file)
    
    if not skip_create:
        create_with_csv(quiz_metainfo_file, vod_csv_file, out_csv_file, customize="PNU888", series_name=series_name)

    if not skip_series_name:
        # 添加父目录到Python路径

        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        from recommender.video_updater import VideoUpdater
        video_updater = VideoUpdater()
        video_updater.update_series_tag_once(series_name, level="初学", tag_list=["科学教育"])