import pandas as pd
import os
import sys
import pysrt
from video_processor import VideoProcessor
from tqdm import tqdm
import jieba
import wave

def tag_video_info_csv_audio_ratio(csv_filename, new_csv_filename):
    df = pd.read_csv(csv_filename)
    content_tagger = ContentTagger()
    for i in tqdm(range(df.shape[0])):
        video_path = df.iloc[i]["FileName"].replace("\\", "/")
        srt_filepath = df.iloc[i]["zh_srt"].replace("\\", "/")
        audio_ratio = content_tagger.tag_audio_ratio(video_path, srt_filepath)
        if audio_ratio != None:
            df.at[i, "audio_ratio"] = audio_ratio
        else:
            df.at[i, "audio_ratio"] = 0
    df.to_csv(new_csv_filename)

def update_video_info_csv_level(csv_filename, new_csv_filename, log_csv_filename=None):
    content_tagger = ContentTagger()
    log_dict = []
    df = pd.read_csv(csv_filename)
    for i in tqdm(range(df.shape[0])):
        vid = df.iloc[i]["VID"]
        # if int(vid) < minid or int(vid) > maxid:
        #     continue
        zhsrt_filename = df.iloc[i]["zh_srt"].replace("\\", "/")
        level, level_ratio = content_tagger.tag_srt_hsklevel(zhsrt_filename)


        if level == None:
            # cannot tag, default to level 2
            new_tag = "level2"
        else:
            new_tag = level
            level_ratio["vid"] = vid
            log_dict.append(level_ratio)

        df.at[i, "level"] = new_tag
    df.to_csv(new_csv_filename)
    if log_csv_filename != None:
        df_log = pd.DataFrame(log_dict)
        df_log.to_csv(log_csv_filename)

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
        
        # init VideoProcessor
        self.video_processor = VideoProcessor()



    def tag_srt_hsklevel(self, srt_filepath):
        try:
            ori_word_list = self.video_processor.split_srt_words(srt_filepath)
        except:
            print ("Cannot split srt words")
            return None, None
        # ori_word_set = set(ori_word_list)

        # get level ratio
        ori_level_ratio = dict()
        for i in range(1, 7):
            hsk_word_list = self.hsk_level_word_dict["level{}".format(i)]
            hsk_word_set = set(hsk_word_list)
            common_word_list = [word for word in ori_word_list if word in hsk_word_set]
            ori_level_ratio["level{}".format(i)] = len(common_word_list) / len(ori_word_list)
        
        print (ori_level_ratio)
            
        level_ratio = dict()
        level_ratio["level1"] = ori_level_ratio["level1"]
        for i in range(2, 6):
            level_ratio["level{}".format(i)] = ori_level_ratio["level{}".format(i)] - ori_level_ratio["level{}".format(i-1)]
        level_ratio["level6"] = 1 - ori_level_ratio["level5"]

        print (level_ratio)
        
        # calc level
        level_value = 0
        for i in range(1, 7):
            level_value += i * level_ratio["level{}".format(i)]
        print (level_value)
        
        hsk_level = round(level_value)
        return hsk_level, level_ratio
        
    def tag_audio_ratio(self, video_path, srt_filepath):
        def time_to_seconds(time):
            return time.hours * 3600 + time.minutes * 60 + time.seconds + time.milliseconds / 1000.0
        try:
            video_path = video_path.replace(" ", "\\ ")
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
            return float(audio_duration) / float(video_duration)
        except:
            return None
            


if __name__ == "__main__":
    # pass
    df = pd.read_csv("video_info_huoshan_update_audio_ratio.csv")
    for i in range(df.shape[0]):
        if df.iloc[i]["audio_ratio"] < 0.3:
            print (df.iloc[i]["FileName"])

    # content_tagger = ContentTagger()
    # content_tagger.tag_audio_ratio("huoshan/琅琊榜/静妃：我力弱，帮不了你什么...#琅琊榜 #胡歌 #一定要看到最后.mp4", "huoshan/琅琊榜/srt_dir/v0332eg10064ct6qmhqljhte03cinb40_Chinese.srt")
    # tag_video_info_csv_audio_ratio("video_info_huoshan.csv", "video_info_huoshan_update_audio_ratio.csv")
    # 
    # print (content_tagger.tag_audio_ratio("huoshan/航拍中国/2021.10.16，雪乡下雪了，你们准备好过冬了吗？#中国雪乡 #旅行大玩家 #原来2021年没有秋天.mp4", "huoshan/航拍中国/srt_dir/v0232eg10064ct6ni8iljht5gshgbcr0_Chinese.srt"))
    # print (content_tagger.tag_srt_hsklevel("/Users/tal/work/lingtok_server/video_process/huoshan/v0332eg10064csvclgqljhtacamhhvu0_Chinese.srt"))
    # update_video_info_csv_level("../video_info_huoshan.csv", "video_info_huoshan_update_level.csv", "level_ratio.csv")