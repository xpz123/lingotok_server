import pandas as pd
import os
import sys
import pysrt
from video_processor import VideoProcessor
from tqdm import tqdm
import jieba
import wave
from datetime import timedelta
from zhon.hanzi import punctuation
import string
# import nltk
import hanlp

def tag_video_info_csv_audio_ratio(csv_filename, new_csv_filename):
    df = pd.read_csv(csv_filename)
    content_tagger = ContentTagger()
    for i in tqdm(range(df.shape[0])):
        video_path = df.iloc[i]["FileName"].replace("\\", "/")
        srt_filepath = df.iloc[i]["zh_srt"].replace("\\", "/")
        audio_ratio, audio_dur = content_tagger.tag_audio_ratio(video_path, srt_filepath)
        if audio_ratio != None:
            df.at[i, "audio_ratio"] = audio_ratio
        else:
            df.at[i, "audio_ratio"] = 0
    df.to_csv(new_csv_filename)

def update_video_info_csv_level(csv_filename, new_csv_filename):
    content_tagger = ContentTagger()
    df = pd.read_csv(csv_filename)
    columns = df.columns.to_list()
    columns.append("level")
    columns.append("audio_ratio")
    columns.append("audio_dur")
    df_list = df.values.tolist()
    for i in tqdm(range(df.shape[0])):
        video_path = df.iloc[i]["FileName"].replace("\\", "/")
        zhsrt_filename = df.iloc[i]["zh_srt"].replace("\\", "/")
        try:
            level, audio_ratio, audio_dur = content_tagger.tag_video_hsklevel(zhsrt_filename, video_path)
        except Exception as e:
            print (e)
            level = -1
            audio_ratio = 0
            audio_dur = 0
        df_list[i].append(level)
        df_list[i].append(audio_ratio)
        df_list[i].append(audio_dur)
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(new_csv_filename, index=False)

class ContentTagger:
    def __init__(self):
        def load_hsk_txt(hsk_file):
            level = hsk_file.split("-")[-1].split(".")[0]
            word_list = list()
            with open(hsk_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line == "":
                        continue
                    if line.find("(") != -1:
                        line = line.split("(")[0]
                    if line.find("……") != -1:
                        word_list.append(line.split("……")[0])
                        word_list.append(line.split("……")[1])
                    else:
                        word_list.append(line)
            return word_list, level
        # load hsk dictionary
        self.hsk_level_word_dict = dict()
        for i in range(1, 7):
            hsk_file = f"hsk_dictionary/hsk-level{i}.txt"
            word_list, level = load_hsk_txt(hsk_file)
            self.hsk_level_word_dict[level] = word_list
            # print (level)
            # print (len(word_list))
        self.ner = hanlp.load(hanlp.pretrained.ner.MSRA_NER_ELECTRA_SMALL_ZH)
        
        self.hsk_level_speed = [120, 150, 180, 220, 260]
        self.hsk_level_ratio = [0.1, 0.3, 0.4, 0.6, 0.7]

        self.hsk_level_weight = {"word": 0.5, "speed": 0.3, "ratio": 0.2}
        
        # init VideoProcessor
        self.video_processor = VideoProcessor()



    def tag_video_hsklevel(self, srt_filepath, video_filepath):
        try:
            ori_word_list = self.split_srt_words(srt_filepath)
        except:
            print ("Cannot split srt words")
            return None
        # ori_word_set = set(ori_word_list)

        # get level ratio
        ori_level_ratio = dict()
        for i in range(1, 7):
            hsk_word_list = self.hsk_level_word_dict["level{}".format(i)]
            hsk_word_set = set(hsk_word_list)
            common_word_list = [word for word in ori_word_list if word in hsk_word_set]
            ori_level_ratio["level{}".format(i)] = len(common_word_list) / len(ori_word_list)
        
        # print (ori_level_ratio)
            
        level_ratio = dict()
        level_ratio["level1"] = ori_level_ratio["level1"]
        for i in range(2, 6):
            level_ratio["level{}".format(i)] = ori_level_ratio["level{}".format(i)] - ori_level_ratio["level{}".format(i-1)]
        level_ratio["level6"] = 1 - ori_level_ratio["level5"]

        # print (level_ratio)
        
        # calc level
        word_level_value = 0
        for i in range(1, 7):
            word_level_value += i * level_ratio["level{}".format(i)]
        print ("word level {}".format(word_level_value))

        audio_speed = self.tag_audio_speed(srt_filepath)
        print ("speed {}".format(audio_speed))

        speed_level_value = 1
        for level_idx, speed in enumerate(self.hsk_level_speed):
            if audio_speed > speed:
                speed_level_value = level_idx + 2

        print ("speed level {}".format(speed_level_value))

        audio_ratio, audio_dur = self.tag_audio_ratio(video_filepath, srt_filepath)
        print ("audio ratio {}".format(audio_ratio))
        ratio_level_value = 1
        for level_idx, ratio in enumerate(self.hsk_level_ratio):
            if audio_ratio > ratio:
                ratio_level_value = level_idx + 2
                
        print ("ratio level {}".format(ratio_level_value))

        hsk_level = word_level_value * self.hsk_level_weight["word"] + speed_level_value * self.hsk_level_weight["speed"] + ratio_level_value * self.hsk_level_weight["ratio"]

        print ("hsk level real {}".format(hsk_level))
        hsk_level = round(hsk_level)
        print ("hsk level {}".format(hsk_level))
        return hsk_level, audio_ratio, audio_dur
        
    def tag_audio_ratio(self, video_path, srt_filepath):
        def time_to_seconds(time):
            return time.hours * 3600 + time.minutes * 60 + time.seconds + time.milliseconds / 1000.0
        try:
            video_path = video_path.replace(" ", "\\ ").replace("&", "\\&")
            os.system("/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg -y -loglevel error -i {} -ac 1 -ar 16000 -f wav test.wav".format(video_path))
            # get duration of test.wav
            with wave.open("test.wav", "rb") as wf:
                video_duration = wf.getnframes() / wf.getframerate()
            os.system("rm test.wav")
            # get audio duration from srt file
            subs = pysrt.open(srt_filepath)
            audio_duration = 0
            for sub in subs:
                audio_duration += time_to_seconds(sub.end) - time_to_seconds(sub.start)
            return float(audio_duration) / float(video_duration), video_duration
        except:
            return -1, -1
    
    def tag_audio_speed(self, srt_filepath):
        """
        Tag audio speed of the video
        Args:
            srt_filepath (str): path of the srt file
        Returns:
            float: characters number per minute
        """
        try:
            subs = pysrt.open(srt_filepath)
            total_characters = 0
            total_seconds = 0
            for sub in subs:
                start_time = sub.start.to_time()
                end_time = sub.end.to_time()
                duration = (timedelta(hours=end_time.hour, minutes=end_time.minute, seconds=end_time.second, milliseconds=end_time.microsecond // 1000) 
                            - timedelta(hours=start_time.hour, minutes=start_time.minute, seconds=start_time.second, milliseconds=start_time.microsecond // 1000)).total_seconds()
                total_seconds += duration
                characters_count = len(sub.text.strip())
                total_characters += characters_count
            
            total_minutes = total_seconds / 60
            characters_per_minute = total_characters / total_minutes if total_minutes > 0 else 0
            return characters_per_minute
        except:
            return None
    
    def split_srt_words(self, subtitle_file):
        subtitles = pysrt.open(subtitle_file)
        word_list = list()
        sent_list = list()
        for sub in subtitles:
            sub_text = sub.text
            seg_list = jieba.cut(sub_text, cut_all=False)
            # remove Chinese punctuation
            seg_list = [word for word in seg_list if word]
            ners = self.ner([seg_list], tasks='ner*')
            del_idx_list = list()
            for item in ners[0]:
                item_idx = item[2]
                del_idx_list.append(item_idx)
            
            for idx, word in enumerate(seg_list):
                if word in punctuation:
                    continue
                if word in string.punctuation:
                    continue
                if word.strip() == "":
                    continue
                if idx in del_idx_list:
                    continue
                word_list.append(word)
            # print (word_list)
            # seg_list = [word for word in seg_list if word not in punctuation]
            # seg_list = [word for word in seg_list if word not in string.punctuation]
            # seg_list = [word for word in seg_list if word.strip() != ""]
        
        return word_list


if __name__ == "__main__":
    update_video_info_csv_level("hw/video_info_hw.csv", "hw/video_info_hw_update_level.csv")
    # content_tagger = ContentTagger()
    # content_tagger.split_srt_words("/Users/tal/work/lingtok_server/video_process/huoshan/test_zh10_srt/v0d32eg10064csvcfn2ljhtd29dgu3rg_Chinese.srt")
    # pass
    # df = pd.read_csv("video_info_huoshan_update_audio_ratio.csv")
    # for i in range(df.shape[0]):
    #     if df.iloc[i]["audio_ratio"] < 0.3:
    #         print (df.iloc[i]["FileName"])

    # content_tagger = ContentTagger()
    # content_tagger.tag_audio_ratio("huoshan/琅琊榜/静妃：我力弱，帮不了你什么...#琅琊榜 #胡歌 #一定要看到最后.mp4", "huoshan/琅琊榜/srt_dir/v0332eg10064ct6qmhqljhte03cinb40_Chinese.srt")
    # tag_video_info_csv_audio_ratio("video_info_huoshan.csv", "video_info_huoshan_update_audio_ratio.csv")
    # 
    # print (content_tagger.tag_audio_ratio("huoshan/航拍中国/2021.10.16，雪乡下雪了，你们准备好过冬了吗？#中国雪乡 #旅行大玩家 #原来2021年没有秋天.mp4", "huoshan/航拍中国/srt_dir/v0232eg10064ct6ni8iljht5gshgbcr0_Chinese.srt"))
    # print (content_tagger.tag_srt_hsklevel("/Users/tal/work/lingtok_server/video_process/huoshan/v0332eg10064csvclgqljhtacamhhvu0_Chinese.srt"))
    # update_video_info_csv_level("../video_info_huoshan.csv", "video_info_huoshan_update_level.csv", "level_ratio.csv")